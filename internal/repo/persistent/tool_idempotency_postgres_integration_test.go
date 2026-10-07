//go:build postgres_integration

package persistent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	agenttool "github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
)

// toolIdempotencyPostgresIntegrationPrefix names the throwaway databases this
// suite creates, so a leaked database identifies its suite.
const toolIdempotencyPostgresIntegrationPrefix = "goagent_tool_idempotency_"

func newToolIdempotencyPostgresIntegrationStore(t *testing.T) (context.Context, *ToolIdempotencyStore) {
	t.Helper()

	pgURL := os.Getenv("GOAGENT_POSTGRES_TEST_URL")
	if pgURL == "" {
		t.Fatal("GOAGENT_POSTGRES_TEST_URL is required for postgres_integration tests")
	}

	suffix := postgresIntegrationSuffix(t, toolIdempotencyPostgresIntegrationPrefix)
	isolatedURL, cleanup := createPostgresIntegrationDatabase(
		t,
		pgURL,
		toolIdempotencyPostgresIntegrationPrefix+suffix,
	)
	t.Cleanup(cleanup)

	pg, err := postgres.New(isolatedURL, postgres.MaxPoolSize(1), postgres.ConnAttempts(1), postgres.ConnTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	t.Cleanup(pg.Close)

	waitForPostgres(t, pg)

	migration := filepath.Join("..", "..", "..", "migrations", "20261007000001_create_tool_idempotency.up.sql")

	data, err := os.ReadFile(migration)
	if err != nil {
		t.Fatalf("read migration %s: %v", migration, err)
	}

	if _, err := pg.Pool.Exec(t.Context(), string(data)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}

	return t.Context(), NewToolIdempotencyStore(pg)
}

func TestToolIdempotencyStoreRoundTrip(t *testing.T) {
	ctx, store := newToolIdempotencyPostgresIntegrationStore(t)

	result := agenttool.Result{
		RunID:      "run-1",
		ToolCallID: "call-1",
		ToolName:   "search",
		Output:     map[string]any{"temperature": "72F", "count": float64(3)},
		Attempts:   2,
	}

	if err := store.Put(ctx, "toolcall:call-1", &result); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, ok, err := store.Get(ctx, "toolcall:call-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !ok {
		t.Fatal("expected the recorded result")
	}

	if got.RunID != result.RunID || got.ToolCallID != result.ToolCallID || got.ToolName != result.ToolName {
		t.Fatalf("identity lost: %+v", got)
	}

	if got.Attempts != result.Attempts {
		t.Fatalf("attempts lost: got %d want %d", got.Attempts, result.Attempts)
	}

	if got.Output["temperature"] != "72F" || got.Output["count"] != float64(3) {
		t.Fatalf("output lost: %+v", got.Output)
	}
}

// The first attempt is the one that had the side effect, so its result is the
// answer every retry must see.
func TestToolIdempotencyStoreFirstWriterWins(t *testing.T) {
	ctx, store := newToolIdempotencyPostgresIntegrationStore(t)

	first := agenttool.Result{RunID: "run-1", ToolCallID: "call-1", ToolName: "send_email", Output: map[string]any{"sent": true}}
	second := agenttool.Result{RunID: "run-1", ToolCallID: "call-1", ToolName: "send_email", Output: map[string]any{"sent": "again"}}

	if err := store.Put(ctx, "toolcall:call-1", &first); err != nil {
		t.Fatalf("first put: %v", err)
	}

	if err := store.Put(ctx, "toolcall:call-1", &second); err != nil {
		t.Fatalf("second put: %v", err)
	}

	got, ok, err := store.Get(ctx, "toolcall:call-1")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}

	if got.Output["sent"] != true {
		t.Fatalf("a retry overwrote the first attempt's result: %+v", got.Output)
	}
}

func TestToolIdempotencyStoreMissIsNotAnError(t *testing.T) {
	ctx, store := newToolIdempotencyPostgresIntegrationStore(t)

	_, ok, err := store.Get(ctx, "toolcall:never-seen")
	if err != nil {
		t.Fatalf("a missing key is a miss, not a failure: %v", err)
	}

	if ok {
		t.Fatal("expected a miss")
	}
}

// A record older than the retention window no longer answers: the store exists
// to resolve a repeat, not to be history.
func TestToolIdempotencyStoreForgetsBeyondRetention(t *testing.T) {
	ctx, store := newToolIdempotencyPostgresIntegrationStore(t)
	store.WithRetention(time.Millisecond)

	result := agenttool.Result{RunID: "run-1", ToolCallID: "call-1", ToolName: "search", Output: map[string]any{"ok": true}}

	if err := store.Put(ctx, "toolcall:call-1", &result); err != nil {
		t.Fatalf("put: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	_, ok, err := store.Get(ctx, "toolcall:call-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ok {
		t.Fatal("a record beyond the retention window must not answer")
	}
}

// Pruning keeps the table bounded without a background job: the write path is
// the only place rows are added.
func TestToolIdempotencyStorePrunesOnWrite(t *testing.T) {
	ctx, store := newToolIdempotencyPostgresIntegrationStore(t)

	// The window must outlast the write's own two round trips, or pruning
	// would delete the row it just wrote; the production window is a day.
	store.WithRetention(50 * time.Millisecond)

	stale := agenttool.Result{RunID: "run-old", ToolCallID: "call-old", ToolName: "search", Output: map[string]any{"ok": true}}

	if err := store.Put(ctx, "toolcall:call-old", &stale); err != nil {
		t.Fatalf("put stale: %v", err)
	}

	time.Sleep(80 * time.Millisecond)

	fresh := agenttool.Result{RunID: "run-new", ToolCallID: "call-new", ToolName: "search", Output: map[string]any{"ok": true}}

	if err := store.Put(ctx, "toolcall:call-new", &fresh); err != nil {
		t.Fatalf("put fresh: %v", err)
	}

	var remaining int

	if err := store.pg.Pool.QueryRow(ctx, `SELECT count(*) FROM agentos_tool_idempotency`).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}

	if remaining != 1 {
		t.Fatalf("expected the stale row to be pruned, %d rows remain", remaining)
	}
}
