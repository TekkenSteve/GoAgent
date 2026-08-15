package conversation

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRuntimePostgresConversationLifecycle(t *testing.T) {
	t.Parallel()

	postgresURL := newConversationTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	runtime := newTestRuntime(ctx, t, Config{PostgresURL: postgresURL, PollInterval: 10 * time.Millisecond})
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	threadID := "thread-" + suffix
	runID := "run-" + suffix
	processID := "process-" + suffix
	accountID := "account-" + suffix
	projectID := "project-" + suffix
	scope := conversationTestScope{
		threadID: threadID, runID: runID, processID: processID,
		accountID: accountID, projectID: projectID,
	}

	snapshot := startConversationTestRun(ctx, t, runtime, &scope, suffix)

	subscription, err := runtime.SubscribeThread(ctx, agentos.ThreadStreamScope{
		ThreadID: threadID, AccountID: accountID, ProjectID: projectID, AfterSequence: snapshot.Cursor,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := subscription.Close(); err != nil {
			t.Errorf("close subscription: %v", err)
		}
	})

	started := scope.ingest(ctx, t, runtime, 1, agentos.ConversationEventRunStarted, map[string]any{})
	assertIdempotencyAndOrdering(ctx, t, runtime, &scope, &started)

	messageID := "assistant-" + suffix
	scope.ingest(ctx, t, runtime, 2, agentos.ConversationEventTextMessageStart, map[string]any{"message_id": messageID, "role": "assistant"})
	scope.ingest(ctx, t, runtime, 3, agentos.ConversationEventTextMessageContent, map[string]any{"message_id": messageID, "delta": "Which scope?"})
	scope.ingest(ctx, t, runtime, 4, agentos.ConversationEventTextMessageEnd, map[string]any{"message_id": messageID, "content": "Which scope?"})
	scope.ingest(ctx, t, runtime, 5, agentos.ConversationEventRunFinished, map[string]any{
		"outcome":   agentos.ConversationOutcomeInterrupt,
		"interrupt": map[string]any{"interrupt_id": "interrupt-" + suffix, "type": "user_input", "prompt": "Which scope?"},
	})

	assertSubscriptionSequence(ctx, t, subscription, 4, 8)
	assertInterruptAndResume(ctx, t, runtime, &scope, suffix)
}

type conversationTestScope struct {
	threadID  string
	runID     string
	processID string
	accountID string
	projectID string
}

func (s *conversationTestScope) ingest(ctx context.Context, t *testing.T, runtime *Runtime, sourceSequence int64, eventType core.EventType, payload map[string]any) agentos.ConversationEvent {
	t.Helper()

	event, err := runtime.IngestEvent(ctx, &agentos.ExternalConversationEvent{
		ThreadID: s.threadID, RunID: s.runID, ProcessID: s.processID,
		AccountID: s.accountID, ProjectID: s.projectID,
		SourceEventID: fmt.Sprintf("source-%d", sourceSequence), SourceSequence: sourceSequence,
		EventType: eventType, OccurredAt: time.Now().UTC(), Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}

	return event
}

func assertIdempotencyAndOrdering(ctx context.Context, t *testing.T, runtime *Runtime, scope *conversationTestScope, started *agentos.ConversationEvent) {
	t.Helper()

	duplicate, err := runtime.IngestEvent(ctx, &agentos.ExternalConversationEvent{
		ThreadID: scope.threadID, RunID: scope.runID, ProcessID: scope.processID,
		AccountID: scope.accountID, ProjectID: scope.projectID,
		SourceEventID: "source-1", SourceSequence: 1,
		EventType: agentos.ConversationEventRunStarted, OccurredAt: time.Now().UTC(), Payload: map[string]any{},
	})
	if err != nil || duplicate.Sequence != started.Sequence {
		t.Fatalf("duplicate source event was not idempotent: event=%+v err=%v", duplicate, err)
	}

	_, err = runtime.IngestEvent(ctx, &agentos.ExternalConversationEvent{
		ThreadID: scope.threadID, RunID: scope.runID, ProcessID: scope.processID,
		AccountID: scope.accountID, ProjectID: scope.projectID,
		SourceEventID: "out-of-order", SourceSequence: 1,
		EventType: "kardcraft.node.started", OccurredAt: time.Now().UTC(), Payload: map[string]any{},
	})
	if !errors.Is(err, ErrOutOfOrderSourceEvent) {
		t.Fatalf("expected out-of-order error, got %v", err)
	}
}

func assertSubscriptionSequence(ctx context.Context, t *testing.T, subscription core.Subscription, first, last int64) {
	t.Helper()

	for expected := first; expected <= last; expected++ {
		select {
		case event := <-subscription.Events():
			if event.Sequence != expected {
				t.Fatalf("subscription sequence: got %d want %d", event.Sequence, expected)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for subscription catch-up")
		}
	}
}

func startConversationTestRun(ctx context.Context, t *testing.T, runtime *Runtime, scope *conversationTestScope, suffix string) agentos.ThreadSnapshot {
	t.Helper()

	run, err := runtime.StartRun(ctx, &agentos.StartConversationRunSpec{
		RunID: scope.runID, ThreadID: scope.threadID, ProcessID: scope.processID,
		AccountID: scope.accountID, ProjectID: scope.projectID, MessageID: "user-" + suffix,
		UserMessage: "Question", IdempotencyKey: "request-" + suffix, RequestedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if run.Status != agentos.ConversationRunPending {
		t.Fatalf("unexpected initial status %q", run.Status)
	}

	snapshot, err := runtime.GetThreadSnapshot(ctx, agentos.ThreadScope{ThreadID: scope.threadID, AccountID: scope.accountID, ProjectID: scope.projectID})
	if err != nil {
		t.Fatal(err)
	}

	if snapshot.Cursor != 3 || len(snapshot.Messages) != 1 {
		t.Fatalf("unexpected initial snapshot cursor=%d messages=%d", snapshot.Cursor, len(snapshot.Messages))
	}

	return snapshot
}

func assertInterruptAndResume(ctx context.Context, t *testing.T, runtime *Runtime, scope *conversationTestScope, suffix string) {
	t.Helper()

	snapshot, err := runtime.GetThreadSnapshot(ctx, agentos.ThreadScope{ThreadID: scope.threadID, AccountID: scope.accountID, ProjectID: scope.projectID})
	if err != nil {
		t.Fatal(err)
	}

	if snapshot.Cursor != 8 || snapshot.Runs[0].Status != agentos.ConversationRunInterrupted || snapshot.Runs[0].Interrupt == nil {
		t.Fatalf("unexpected interrupt snapshot: %+v", snapshot)
	}

	resumeRun, err := runtime.StartRun(ctx, &agentos.StartConversationRunSpec{
		RunID: "resume-" + suffix, ThreadID: scope.threadID, ProcessID: scope.processID,
		AccountID: scope.accountID, ProjectID: scope.projectID, MessageID: "resume-user-" + suffix,
		UserMessage: "Chapter 2", IdempotencyKey: "resume-request-" + suffix,
		Resume: &agentos.ConversationResume{InterruptID: "interrupt-" + suffix, Response: "Chapter 2"}, RequestedAt: time.Now().UTC(),
	})
	if err != nil || resumeRun.RunID == scope.runID {
		t.Fatalf("resume must create a new run: run=%+v err=%v", resumeRun, err)
	}
}

// TestRuntimePollingDeliversAcrossRuntimes verifies that a subscription on one
// runtime picks up events appended by a separate writer runtime purely through
// Postgres polling: there is no live transport after the Redis migration.
func TestRuntimePollingDeliversAcrossRuntimes(t *testing.T) {
	t.Parallel()

	postgresURL := newConversationTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reader := newTestRuntime(ctx, t, Config{PostgresURL: postgresURL, PollInterval: 10 * time.Millisecond})
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close reader runtime: %v", err)
		}
	})

	suffix := fmt.Sprintf("poll-%d", time.Now().UnixNano())
	threadID := "thread-" + suffix
	runID := "run-" + suffix
	accountID := "account-" + suffix
	projectID := "project-" + suffix
	scope := conversationTestScope{
		threadID: threadID, runID: runID, processID: "process-" + suffix,
		accountID: accountID, projectID: projectID,
	}

	snapshot := startConversationTestRun(ctx, t, reader, &scope, suffix)

	subscription, err := reader.SubscribeThread(ctx, agentos.ThreadStreamScope{
		ThreadID: threadID, AccountID: accountID, ProjectID: projectID, AfterSequence: snapshot.Cursor,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := subscription.Close(); err != nil {
			t.Errorf("close subscription: %v", err)
		}
	})

	writer := newTestRuntime(ctx, t, Config{PostgresURL: postgresURL, PollInterval: 10 * time.Millisecond})
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Errorf("close writer runtime: %v", err)
		}
	})

	gapEvents := writeGapEvents(ctx, t, writer, &scope, suffix)

	assertSubscriptionSequenceBefore(t, subscription, snapshot.Cursor+1, snapshot.Cursor+int64(len(gapEvents)), 5*time.Second)
}

func writeGapEvents(ctx context.Context, t *testing.T, writer *Runtime, scope *conversationTestScope, suffix string) []agentos.ConversationEvent {
	t.Helper()

	events := make([]agentos.ConversationEvent, 0, 2)

	for sourceSequence, eventType := range []core.EventType{"kardcraft.node.started", "kardcraft.node.completed"} {
		stored, err := writer.IngestEvent(ctx, &agentos.ExternalConversationEvent{
			ThreadID: scope.threadID, RunID: scope.runID, ProcessID: scope.processID,
			AccountID: scope.accountID, ProjectID: scope.projectID,
			SourceEventID:  fmt.Sprintf("source-gap-%d-%s", sourceSequence, suffix),
			SourceSequence: int64(sourceSequence + 2), EventType: eventType,
			OccurredAt: time.Now().UTC(), Payload: map[string]any{"node_id": "node-1"},
		})
		if err != nil {
			t.Fatal(err)
		}

		events = append(events, stored)
	}

	return events
}

func assertSubscriptionSequenceBefore(t *testing.T, subscription core.Subscription, first, last int64, timeout time.Duration) {
	t.Helper()

	for expected := first; expected <= last; expected++ {
		select {
		case event := <-subscription.Events():
			if event.Sequence != expected {
				t.Fatalf("gap recovery sequence = %d, want %d", event.Sequence, expected)
			}
		case <-time.After(timeout):
			t.Fatalf("timed out recovering sequence %d", expected)
		}
	}
}

func requireRuntime(t *testing.T, runtimeAPI agentos.ConversationRuntime) *Runtime {
	t.Helper()

	runtime, ok := runtimeAPI.(*Runtime)
	if !ok {
		t.Fatalf("runtime type = %T, want *conversation.Runtime", runtimeAPI)
	}

	return runtime
}

func newTestRuntime(ctx context.Context, t *testing.T, config Config) *Runtime {
	t.Helper()

	runtimeAPI, err := NewRuntime(ctx, config)
	if err != nil {
		t.Fatal(err)
	}

	return requireRuntime(t, runtimeAPI)
}

const (
	// conversationTestDBNameBytes is Postgres's identifier length limit
	// (NAMEDATALEN-1); names longer than this are truncated by the server, so
	// the test database name is budgeted to fit.
	conversationTestDBNameBytes = 63

	// conversationTestDBTokenHexLen is the hex length of time.Now().UnixNano(),
	// used to keep database names unique across runs without an extra
	// dependency.
	conversationTestDBTokenHexLen = 16

	// postgresAdminPingAttempts bounds how long newConversationTestDB waits for
	// the Postgres server to answer before failing the test.
	postgresAdminPingAttempts = 30
)

// newConversationTestDB creates a fresh, isolated Postgres database for one
// test, applies the conversation migrations to it, and returns a URL pointing
// at the new database. The database is dropped when the test finishes.
//
// Each Postgres test gets its own database so that serializable IngestEvent
// transactions from different tests cannot trip one another's SSI
// rw-antidependencies (SQLSTATE 40001): tests that are safe in isolation
// deadlock-then-abort when they run in parallel against the same database.
func newConversationTestDB(t *testing.T) string {
	t.Helper()

	adminURL := os.Getenv("AGENTOS_TEST_PG_URL")
	if adminURL == "" {
		t.Skip("AGENTOS_TEST_PG_URL is not set")
	}

	admin, err := pgxpool.New(context.Background(), adminURL)
	if err != nil {
		t.Fatalf("connect postgres admin: %v", err)
	}

	t.Cleanup(admin.Close)

	waitForPostgresAdmin(t, admin)

	dbName := conversationTestDatabaseName(t)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		t.Fatalf("create conversation test database %q: %v", dbName, err)
	}

	isolated, err := url.Parse(adminURL)
	if err != nil {
		t.Fatalf("parse postgres test URL: %v", err)
	}

	isolated.Path = "/" + dbName
	isolatedURL := isolated.String()

	applyConversationMigrations(t, isolatedURL)

	t.Cleanup(func() {
		dropConversationTestDatabase(t, admin, dbName)
	})

	return isolatedURL
}

// waitForPostgresAdmin pings the admin pool until the Postgres server answers
// queries or the wait budget runs out. pgxpool connects lazily, so CREATE
// DATABASE must wait for the server to be reachable first.
func waitForPostgresAdmin(t *testing.T, admin *pgxpool.Pool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for range postgresAdminPingAttempts {
		if err := admin.Ping(ctx); err == nil {
			return
		}

		time.Sleep(250 * time.Millisecond)
	}

	t.Fatal("postgres did not become ready")
}

// applyConversationMigrations runs the conversation schema migrations against
// a fresh test database, in dependency order (the event-outbox table
// references the conversation-events table).
func applyConversationMigrations(t *testing.T, isolatedURL string) {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), isolatedURL)
	if err != nil {
		t.Fatalf("connect conversation test database: %v", err)
	}

	t.Cleanup(pool.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	for _, migration := range []string{
		"20260507000001_create_messages.up.sql",
		"20260725000001_create_agentos_conversations.up.sql",
		"20260726000001_create_agentos_conversation_event_outbox.up.sql",
	} {
		data, err := os.ReadFile(filepath.Join("..", "..", "migrations", migration))
		if err != nil {
			t.Fatalf("read migration %s: %v", migration, err)
		}

		if _, err := pool.Exec(ctx, string(data)); err != nil {
			t.Fatalf("apply migration %s: %v", migration, err)
		}
	}
}

// dropConversationTestDatabase terminates any connections the runtimes of a
// test may still hold to its database, then drops the database. Best-effort:
// a leftover database is logged rather than failing the test, so a cleanup
// error does not mask the test's own result.
func dropConversationTestDatabase(t *testing.T, admin *pgxpool.Pool, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := admin.Exec(ctx, `
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = $1 AND pid <> pg_backend_pid()`, dbName); err != nil {
		t.Logf("terminate conversation test backends: %v", err)
	}

	if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		t.Logf("drop conversation test database %q: %v", dbName, err)
	}
}

// conversationTestDatabaseName derives a unique, Postgres-safe database name
// for a test, e.g. conv_testruntimepostgresconversationlifecycle_1a2b3c.
func conversationTestDatabaseName(t *testing.T) string {
	t.Helper()

	name := strings.ToLower(strings.NewReplacer("/", "_", "-", "_", ".", "_").Replace(t.Name()))
	token := fmt.Sprintf("%x", time.Now().UnixNano())

	budget := conversationTestDBNameBytes - len("conv_") - len("_") - conversationTestDBTokenHexLen
	if len(name) > budget {
		name = name[:budget]
	}

	return "conv_" + name + "_" + token
}
