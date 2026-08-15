//go:build postgres_integration

package persistent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
)

// TestAgentOSRunEventPostgresDurablePersistence locks the run event projection
// sink: the persisted cursor, idempotent re-append on the bus offset, the
// in-process validation, and the schema-level CHECK / UNIQUE / append-only
// guards on direct SQL.
func TestAgentOSRunEventPostgresDurablePersistence(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSRunEventPostgresIntegrationDB(t)
	assertPostgresTables(t, pg, "agentos_run_events")
	assertPostgresTrigger(t, pg, "agentos_run_events_append_only")

	repo := NewAgentOSRunEventRepo(pg)
	runID := "run-" + suffix

	cursor, err := repo.LastRunEventSequence(ctx, runID)
	if err != nil {
		t.Fatalf("LastRunEventSequence empty: %v", err)
	}
	if cursor != 0 {
		t.Fatalf("empty cursor = %d, want 0", cursor)
	}

	first := &agentoscore.Event{
		EventType: agentoscore.EventRunStarted,
		RunID:     runID,
		ThreadID:  "sess-1",
		Sequence:  1,
		Timestamp: time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC),
		Source:    "$agentos:run:acme:" + runID,
		Payload:   map[string]any{"thread_id": "sess-1"},
	}
	if err := repo.AppendRunEvent(ctx, first); err != nil {
		t.Fatalf("AppendRunEvent first: %v", err)
	}
	if first.EventID != runID+":1" {
		t.Fatalf("first event id = %q, want %q", first.EventID, runID+":1")
	}

	// Re-append of the same bus offset is a no-op, not an error.
	replay := *first
	if err := repo.AppendRunEvent(ctx, &replay); err != nil {
		t.Fatalf("AppendRunEvent replay: %v", err)
	}

	second := &agentoscore.Event{
		EventType: agentoscore.EventToolCallStarted,
		RunID:     runID,
		Sequence:  2,
		Timestamp: time.Date(2026, 8, 15, 12, 0, 1, 0, time.UTC),
	}
	if err := repo.AppendRunEvent(ctx, second); err != nil {
		t.Fatalf("AppendRunEvent second: %v", err)
	}

	cursor, err = repo.LastRunEventSequence(ctx, runID)
	if err != nil {
		t.Fatalf("LastRunEventSequence after: %v", err)
	}
	if cursor != 2 {
		t.Fatalf("cursor after two events = %d, want 2", cursor)
	}

	// A different event at the same offset is also a no-op: the bus offset is
	// the idempotency token, so the original row wins.
	duplicate := *first
	duplicate.EventType = agentoscore.EventRunFailed
	if err := repo.AppendRunEvent(ctx, &duplicate); err != nil {
		t.Fatalf("AppendRunEvent duplicate: %v", err)
	}

	var rowCount int
	if err := pg.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM agentos_run_events WHERE run_id = $1`, runID).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 2 {
		t.Fatalf("row count = %d, want 2", rowCount)
	}

	// In-process validation rejects malformed events before SQL.
	invalid := *first
	invalid.RunID = ""
	if err := repo.AppendRunEvent(ctx, &invalid); !errors.Is(err, agentoscore.ErrInvalidRunEvent) {
		t.Fatalf("AppendRunEvent missing run id error = %v, want ErrInvalidRunEvent", err)
	}
	invalid = *first
	invalid.EventType = ""
	if err := repo.AppendRunEvent(ctx, &invalid); !errors.Is(err, agentoscore.ErrInvalidRunEvent) {
		t.Fatalf("AppendRunEvent missing event type error = %v, want ErrInvalidRunEvent", err)
	}
	invalid = *first
	invalid.Sequence = 0
	if err := repo.AppendRunEvent(ctx, &invalid); !errors.Is(err, agentoscore.ErrInvalidRunEvent) {
		t.Fatalf("AppendRunEvent zero sequence error = %v, want ErrInvalidRunEvent", err)
	}
	if err := repo.AppendRunEvent(ctx, nil); !errors.Is(err, agentoscore.ErrInvalidRunEvent) {
		t.Fatalf("AppendRunEvent nil error = %v, want ErrInvalidRunEvent", err)
	}

	// Direct SQL is rejected by the schema guards even though the repo checks
	// the same invariants in-process.
	if _, err := pg.Pool.Exec(ctx, `
INSERT INTO agentos_run_events (run_id, sequence, event_id, event_type, occurred_at)
VALUES ($1, 3, $2, '', NOW())`, runID, runID+":3"); err == nil {
		t.Fatal("direct insert with empty event_type succeeded, want CHECK rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `
INSERT INTO agentos_run_events (run_id, sequence, event_id, event_type, occurred_at)
VALUES ($1, 0, $2, 'run.started', NOW())`, runID, runID+":0"); err == nil {
		t.Fatal("direct insert with sequence 0 succeeded, want CHECK rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `
INSERT INTO agentos_run_events (run_id, sequence, event_id, event_type, occurred_at)
VALUES ($1, 3, $2, 'run.started', NOW())`, "other-"+runID, runID+":1"); err == nil {
		t.Fatal("direct insert with duplicate event_id succeeded, want UNIQUE rejection")
	}

	// The append-only trigger rejects UPDATE and DELETE.
	if _, err := pg.Pool.Exec(ctx, `UPDATE agentos_run_events SET event_type = 'run.failed' WHERE run_id = $1 AND sequence = 1`, runID); err == nil {
		t.Fatal("direct update succeeded, want append-only trigger rejection")
	}
	if _, err := pg.Pool.Exec(ctx, `DELETE FROM agentos_run_events WHERE run_id = $1 AND sequence = 1`, runID); err == nil {
		t.Fatal("direct delete succeeded, want append-only trigger rejection")
	}
}

func newAgentOSRunEventPostgresIntegrationDB(t *testing.T) (context.Context, *postgres.Postgres, string) {
	t.Helper()

	pgURL := os.Getenv("GOAGENT_POSTGRES_TEST_URL")
	if pgURL == "" {
		t.Fatal("GOAGENT_POSTGRES_TEST_URL is required for postgres_integration tests")
	}

	suffix := postgresIntegrationSuffix(t)
	isolatedURL, cleanup := createPostgresIntegrationDatabase(
		t,
		pgURL,
		postgresIntegrationDatabasePrefix+suffix,
	)
	t.Cleanup(cleanup)

	pg, err := postgres.New(isolatedURL, postgres.MaxPoolSize(1), postgres.ConnAttempts(1), postgres.ConnTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	t.Cleanup(pg.Close)
	waitForPostgres(t, pg)
	applyAgentOSRunEventMigrations(t, pg)

	return t.Context(), pg, suffix
}

func applyAgentOSRunEventMigrations(t *testing.T, pg *postgres.Postgres) {
	t.Helper()

	for _, migration := range []string{
		"20260815000001_create_agentos_run_events.up.sql",
	} {
		path := filepath.Join("..", "..", "..", "migrations", migration)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", migration, err)
		}
		if _, err := pg.Pool.Exec(t.Context(), string(data)); err != nil {
			t.Fatalf("apply migration %s: %v", migration, err)
		}
	}
}
