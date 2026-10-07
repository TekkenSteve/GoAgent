package conversation

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
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

// conversationTestSourceID names a source event inside one run's stream: source
// ids are unique per thread, so two runs of a test must not share them.
func conversationTestSourceID(runID string, sourceSequence int64) string {
	return fmt.Sprintf("source-%s-%d", runID, sourceSequence)
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
		SourceEventID: conversationTestSourceID(s.runID, sourceSequence), SourceSequence: sourceSequence,
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
		SourceEventID: conversationTestSourceID(scope.runID, 1), SourceSequence: 1,
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

// TestRuntimePostgresEventOutboxEnqueue locks the enqueue half of the
// transactional outbox: every event that becomes durable is recorded for
// publication in the same transaction, and a rolled-back ingest leaves nothing
// behind — no outbox row for an event that never became durable.
func TestRuntimePostgresEventOutboxEnqueue(t *testing.T) {
	t.Parallel()

	postgresURL := newConversationTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, postgresURL)
	if err != nil {
		t.Fatalf("connect conversation test database: %v", err)
	}

	t.Cleanup(pool.Close)

	suffix := fmt.Sprintf("outbox-%d", time.Now().UnixNano())
	scope := conversationTestScope{
		threadID: "thread-" + suffix, runID: "run-" + suffix, processID: "process-" + suffix,
		accountID: "account-" + suffix, projectID: "project-" + suffix,
	}

	enabled := newTestRuntime(ctx, t, Config{
		PostgresURL: postgresURL, PollInterval: 10 * time.Millisecond, EventOutbox: true,
	})
	t.Cleanup(func() {
		if err := enabled.Close(); err != nil {
			t.Errorf("close outbox runtime: %v", err)
		}
	})

	// StartRun writes the run plus its three user-message events; the ingest
	// adds a fourth event.
	startConversationTestRun(ctx, t, enabled, &scope, suffix)
	scope.ingest(ctx, t, enabled, 2, agentos.ConversationEventRunStarted, map[string]any{})

	assertOutboxSequences(ctx, t, pool, scope.threadID, []int64{1, 2, 3, 4})

	// A refused ingest must not enqueue: order rejection rolls the transaction
	// back, outbox row included.
	if _, err := enabled.IngestEvent(ctx, &agentos.ExternalConversationEvent{
		ThreadID: scope.threadID, RunID: scope.runID, ProcessID: scope.processID,
		AccountID: scope.accountID, ProjectID: scope.projectID,
		SourceEventID: "source-stale", SourceSequence: 1,
		EventType: agentos.ConversationEventRunFinished, OccurredAt: time.Now().UTC(),
		Payload: map[string]any{"outcome": agentos.ConversationOutcomeNormal},
	}); !errors.Is(err, ErrOutOfOrderSourceEvent) {
		t.Fatalf("stale source event error = %v, want ErrOutOfOrderSourceEvent", err)
	}

	assertOutboxSequences(ctx, t, pool, scope.threadID, []int64{1, 2, 3, 4})

	// The outbox row is a claim check: it carries no payload column, so the
	// event bytes exist exactly once, in agentos_conversation_events.
	var payloadColumns int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM information_schema.columns
WHERE table_name = 'agentos_conversation_event_outbox' AND column_name IN ('payload', 'event')`).Scan(&payloadColumns); err != nil {
		t.Fatalf("inspect outbox columns: %v", err)
	}

	if payloadColumns != 0 {
		t.Fatalf("outbox has %d payload-bearing columns, want 0", payloadColumns)
	}
}

// TestRuntimePostgresEventOutboxDisabled locks the opt-in: a runtime that never
// enabled the outbox must leave it alone, or a deployment with no drainer would
// accumulate rows nothing consumes.
func TestRuntimePostgresEventOutboxDisabled(t *testing.T) {
	t.Parallel()

	postgresURL := newConversationTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, postgresURL)
	if err != nil {
		t.Fatalf("connect conversation test database: %v", err)
	}

	t.Cleanup(pool.Close)

	suffix := fmt.Sprintf("plain-%d", time.Now().UnixNano())
	scope := conversationTestScope{
		threadID: "plain-thread-" + suffix, runID: "plain-run-" + suffix, processID: "plain-" + suffix,
		accountID: "account-" + suffix, projectID: "project-" + suffix,
	}

	disabled := newTestRuntime(ctx, t, Config{PostgresURL: postgresURL, PollInterval: 10 * time.Millisecond})
	t.Cleanup(func() {
		if err := disabled.Close(); err != nil {
			t.Errorf("close plain runtime: %v", err)
		}
	})

	startConversationTestRun(ctx, t, disabled, &scope, suffix)

	var plainRows int
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM agentos_conversation_event_outbox WHERE thread_id = $1`, scope.threadID).Scan(&plainRows); err != nil {
		t.Fatalf("count plain outbox rows: %v", err)
	}

	if plainRows != 0 {
		t.Fatalf("outbox rows for a runtime without EventOutbox = %d, want 0", plainRows)
	}
}

// assertOutboxSequences compares the outbox's queued sequences for a thread
// against the expected order. The outbox must mirror the event stream exactly:
// a missing row is a lost publication, an extra row is a duplicate.
func assertOutboxSequences(ctx context.Context, t *testing.T, pool *pgxpool.Pool, threadID string, want []int64) {
	t.Helper()

	rows, err := pool.Query(ctx, `
SELECT sequence, attempts, last_error
FROM agentos_conversation_event_outbox
WHERE thread_id = $1
ORDER BY sequence`, threadID)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	defer rows.Close()

	got := make([]int64, 0, len(want))

	for rows.Next() {
		var (
			sequence  int64
			attempts  int
			lastError string
		)

		if err := rows.Scan(&sequence, &attempts, &lastError); err != nil {
			t.Fatalf("scan outbox row: %v", err)
		}

		if attempts != 0 || lastError != "" {
			t.Fatalf("fresh outbox row %d = attempts %d, last_error %q; want a pristine row", sequence, attempts, lastError)
		}

		got = append(got, sequence)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox: %v", err)
	}

	if !slices.Equal(got, want) {
		t.Fatalf("outbox sequences = %v, want %v", got, want)
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

	data, err := os.ReadFile(filepath.Join("..", "..", "migrations", "20261010000001_baseline.up.sql"))
	if err != nil {
		t.Fatalf("read baseline schema: %v", err)
	}

	if _, err := pool.Exec(ctx, string(data)); err != nil {
		t.Fatalf("apply baseline schema: %v", err)
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

// conversationTestThread is a running thread with one run open, ready for a
// scenario to feed it events.
type conversationTestThread struct {
	ctx       context.Context
	runtime   *Runtime
	scope     *conversationTestScope
	suffix    string
	threadID  string
	accountID string
	projectID string
	processID string
}

// newConversationTestThread provisions a database, a runtime and one running
// run on a fresh thread. Every scenario starts this way, so it is stated once.
func newConversationTestThread(t *testing.T) conversationTestThread {
	t.Helper()

	postgresURL := newConversationTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	runtime := newTestRuntime(ctx, t, Config{PostgresURL: postgresURL, PollInterval: 10 * time.Millisecond})
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	threadID := "thread-" + suffix
	accountID := "account-" + suffix
	projectID := "project-" + suffix
	processID := "process-" + suffix
	scope := &conversationTestScope{
		threadID: threadID, runID: "run-" + suffix, processID: processID,
		accountID: accountID, projectID: projectID,
	}

	startConversationTestRun(ctx, t, runtime, scope, suffix)
	scope.ingest(ctx, t, runtime, 1, agentos.ConversationEventRunStarted, map[string]any{})

	return conversationTestThread{
		ctx: ctx, runtime: runtime, scope: scope, suffix: suffix,
		threadID: threadID, accountID: accountID, projectID: projectID, processID: processID,
	}
}

// TestRuntimeSteerAdmitsWhileRunningAndGatesTheStream pins the invariant split:
// admission accepts a second run while one is on the wire (the steer and
// interrupt handoff need that), and the RUN_STARTED transition is what decides
// when it may stream — it is parked while the thread is taken, not refused,
// because a producer that delivered the fact once would otherwise lose the
// turn.
func TestRuntimeSteerAdmitsWhileRunningAndGatesTheStream(t *testing.T) {
	t.Parallel()

	thread := newConversationTestThread(t)

	steerID := "steer-" + thread.suffix

	steerRun, err := thread.runtime.StartRun(thread.ctx, &agentos.StartConversationRunSpec{
		RunID: steerID, ThreadID: thread.threadID, ProcessID: thread.processID,
		AccountID: thread.accountID, ProjectID: thread.projectID, MessageID: "steer-user-" + thread.suffix,
		UserMessage: "Actually make them harder", IdempotencyKey: "steer-request-" + thread.suffix,
		RequestedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("a running thread must still admit the next run: %v", err)
	}

	if steerRun.Status != agentos.ConversationRunPending {
		t.Fatalf("admitted run status = %q, want pending", steerRun.Status)
	}

	_, err = thread.runtime.IngestEvent(thread.ctx, &agentos.ExternalConversationEvent{
		ThreadID: thread.threadID, RunID: steerID, ProcessID: thread.processID,
		AccountID: thread.accountID, ProjectID: thread.projectID,
		SourceEventID: "steer-source-1", SourceSequence: 1,
		EventType: agentos.ConversationEventRunStarted, OccurredAt: time.Now().UTC(), Payload: map[string]any{},
	})
	if err != nil {
		t.Fatalf("a steer turn's start must be accepted while the thread is taken: %v", err)
	}

	// Accepted does not mean streaming: the turn waits for the thread, and the
	// predecessor is still the only running run.
	if got := runStatus(thread.ctx, t, thread.runtime, steerID); got != agentos.ConversationRunPending {
		t.Fatalf("parked turn status = %q, want pending", got)
	}

	if got := runStatus(thread.ctx, t, thread.runtime, thread.scope.runID); got != agentos.ConversationRunRunning {
		t.Fatalf("predecessor status = %q, want running", got)
	}

	thread.scope.ingest(thread.ctx, t, thread.runtime, 2, agentos.ConversationEventRunFinished, map[string]any{
		"outcome":            agentos.ConversationOutcomeNormal,
		"termination_reason": "steered",
	})

	// The sweeper is what starts the parked turn, without the producer
	// re-delivering anything.
	if _, projected, err := thread.runtime.sweepOnce(thread.ctx); err != nil || projected != 1 {
		t.Fatalf("sweep must start the parked turn: projected=%d err=%v", projected, err)
	}

	if got := runStatus(thread.ctx, t, thread.runtime, steerID); got != agentos.ConversationRunRunning {
		t.Fatalf("parked turn status = %q, want running", got)
	}
}

// TestRuntimeDuplicateAdmissionReturnsTheOriginalRun keeps StartRun idempotent
// by admission identity after the open-run index no longer covers pending runs.
func TestRuntimeDuplicateAdmissionReturnsTheOriginalRun(t *testing.T) {
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
	spec := &agentos.StartConversationRunSpec{
		RunID: "run-" + suffix, ThreadID: "thread-" + suffix, ProcessID: "process-" + suffix,
		AccountID: "account-" + suffix, ProjectID: "project-" + suffix, MessageID: "user-" + suffix,
		UserMessage: "Question", IdempotencyKey: "request-" + suffix, RequestedAt: time.Now().UTC(),
	}

	first, err := runtime.StartRun(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}

	replay, err := runtime.StartRun(ctx, spec)
	if err != nil {
		t.Fatalf("replayed admission must be idempotent: %v", err)
	}

	if replay.RunID != first.RunID {
		t.Fatalf("replayed admission returned run %q, want %q", replay.RunID, first.RunID)
	}
}

// ingestAssistantMessages feeds one run's version of a message stream: a
// completed message, a streamed one, and optionally a second streamed message
// the run is steered away from. Each run of a thread re-emits the same message
// ids with its own content, which is what makes a replay a replay.
// assistantReplayContent is what one run says about the message it re-emits:
// the delta it streams and the final content it declares.
//
// The two are separate because the replay this test covers is exactly that: an
// earlier run streamed a prefix and closed the message with the whole text, and
// the run that resumes the thread re-emits the same message id with more
// content. Collapsing them into one value would test a different scenario.
type assistantReplayContent struct {
	delta string
	end   string
}

// ingestAssistantMessages has one run emit the two assistant messages the
// replay scenario is about: a message that is closed within the run, and a
// message the run leaves open for the run that supersedes it.
func ingestAssistantMessages(
	ctx context.Context,
	t *testing.T,
	scope *conversationTestScope,
	runtime *Runtime,
	fromSequence int64,
	finishedID, openID string,
	finished assistantReplayContent,
	openDelta string,
) {
	t.Helper()

	scope.ingest(ctx, t, runtime, fromSequence, agentos.ConversationEventTextMessageStart, map[string]any{"message_id": finishedID, "role": "assistant"})
	scope.ingest(ctx, t, runtime, fromSequence+1, agentos.ConversationEventTextMessageContent, map[string]any{"message_id": finishedID, "delta": finished.delta})
	scope.ingest(ctx, t, runtime, fromSequence+2, agentos.ConversationEventTextMessageEnd, map[string]any{"message_id": finishedID, "content": finished.end})
	scope.ingest(ctx, t, runtime, fromSequence+3, agentos.ConversationEventTextMessageStart, map[string]any{"message_id": openID, "role": "assistant"})
	scope.ingest(ctx, t, runtime, fromSequence+4, agentos.ConversationEventTextMessageContent, map[string]any{"message_id": openID, "delta": openDelta})
}

// TestRuntimeAppliesReplayedMessagesIdempotently covers the durable record's
// side of a resumed thread: LangGraph re-emits an earlier run's messages with
// their original ids, so the second run's stream must extend the record instead
// of failing, and a replay must close a message its superseded run left open.
func TestRuntimeAppliesReplayedMessagesIdempotently(t *testing.T) {
	t.Parallel()

	thread := newConversationTestThread(t)

	// The first run leaves one message complete and a second one streaming when
	// it is steered away.
	finishedID := "assistant-finished-" + thread.suffix
	openID := "assistant-open-" + thread.suffix

	ingestAssistantMessages(thread.ctx, t, thread.scope, thread.runtime, 2, finishedID, openID,
		assistantReplayContent{delta: "first half ", end: "first half second half"}, "partial")
	thread.scope.ingest(thread.ctx, t, thread.runtime, 7, agentos.ConversationEventRunFinished, map[string]any{
		"outcome": agentos.ConversationOutcomeNormal, "termination_reason": "steered",
	})

	steerScope := &conversationTestScope{
		threadID: thread.threadID, runID: "steer-" + thread.suffix, processID: thread.processID,
		accountID: thread.accountID, projectID: thread.projectID,
	}
	if _, err := thread.runtime.StartRun(thread.ctx, &agentos.StartConversationRunSpec{
		RunID: steerScope.runID, ThreadID: thread.threadID, ProcessID: thread.processID,
		AccountID: thread.accountID, ProjectID: thread.projectID, MessageID: "steer-user-" + thread.suffix,
		UserMessage: "Correction", IdempotencyKey: "steer-request-" + thread.suffix, RequestedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	steerScope.ingest(thread.ctx, t, thread.runtime, 1, agentos.ConversationEventRunStarted, map[string]any{})
	ingestAssistantMessages(thread.ctx, t, steerScope, thread.runtime, 2, finishedID, openID,
		assistantReplayContent{delta: "first half second half", end: "first half second half"}, "partial final")
	steerScope.ingest(thread.ctx, t, thread.runtime, 7, agentos.ConversationEventTextMessageEnd, map[string]any{"message_id": openID, "content": "partial final answer"})

	steerOnlyID := "assistant-steer-only-" + thread.suffix
	steerScope.ingest(thread.ctx, t, thread.runtime, 8, agentos.ConversationEventTextMessageStart, map[string]any{"message_id": steerOnlyID, "role": "assistant"})

	_, err := thread.runtime.IngestEvent(thread.ctx, &agentos.ExternalConversationEvent{
		ThreadID: thread.threadID, RunID: steerScope.runID, ProcessID: thread.processID,
		AccountID: thread.accountID, ProjectID: thread.projectID,
		SourceEventID: "steer-duplicate-start", SourceSequence: 9,
		EventType: agentos.ConversationEventTextMessageStart, OccurredAt: time.Now().UTC(),
		Payload: map[string]any{"message_id": steerOnlyID, "role": "assistant"},
	})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("a second start inside one run must stay refused: %v", err)
	}

	assertReplayedMessages(thread.ctx, t, thread.runtime, thread.threadID, thread.accountID, thread.projectID, map[string]replayedMessageExpectation{
		finishedID: {content: "first half second half", runID: thread.scope.runID},
		openID:     {content: "partial final answer", runID: thread.scope.runID},
	})
}

// replayedMessageExpectation is the durable shape one replayed message must
// settle into: the final content, completed, and owned by the run that started
// it rather than the superseded one that re-emitted it.
type replayedMessageExpectation struct {
	content string
	runID   string
}

func assertReplayedMessages(
	ctx context.Context,
	t *testing.T,
	runtime *Runtime,
	threadID, accountID, projectID string,
	expected map[string]replayedMessageExpectation,
) {
	t.Helper()

	snapshot, err := runtime.GetThreadSnapshot(ctx, agentos.ThreadScope{ThreadID: threadID, AccountID: accountID, ProjectID: projectID})
	if err != nil {
		t.Fatal(err)
	}

	messages := make(map[string]agentos.ConversationMessage, len(snapshot.Messages))

	for i := range snapshot.Messages {
		message := &snapshot.Messages[i]
		messages[message.MessageID] = *message
	}

	for messageID, want := range expected {
		got := messages[messageID]
		if got.Content != want.content || got.Status != "completed" || got.RunID != want.runID {
			t.Fatalf("replayed message %s = %+v, want content %q completed on run %s", messageID, got, want.content, want.runID)
		}
	}
}

// At-least-once delivery means the same upstream event can arrive twice at
// once. The runtime promises that a source event is ingested exactly once, and
// concurrent *writers of disjoint sequences* are legitimately rejected as out
// of order — that is the ordering contract, not contention. The contention
// this test creates is the real one: several writers delivering the same
// events, where the database has to adjudicate which insert wins and the
// runtime must answer every caller identically, including the losers.
func TestRuntimePostgresConcurrentDuplicateDeliveryIsIdempotent(t *testing.T) {
	t.Parallel()

	postgresURL := newConversationTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var replays int64

	runtime := newTestRuntime(ctx, t, Config{
		PostgresURL: postgresURL,
		MaxConns:    8,
		OnSerializationRetry: func(int, error) {
			atomic.AddInt64(&replays, 1)
		},
	})
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	suffix := fmt.Sprintf("duplicate-%d", time.Now().UnixNano())
	scope := conversationTestScope{
		threadID: "thread-" + suffix, runID: "run-" + suffix, processID: "process-" + suffix,
		accountID: "account-" + suffix, projectID: "project-" + suffix,
	}

	startConversationTestRun(ctx, t, runtime, &scope, suffix)

	const (
		writers    = 8
		deliveries = 3
		sequence   = int64(7)
	)

	observations := deliverDuplicateEventConcurrently(ctx, t, runtime, &scope, writers, deliveries, sequence)

	// Every caller saw the same event, whichever one inserted it.
	require.Len(t, observations.events, writers*deliveries)

	for _, event := range observations.events {
		require.Equal(t, observations.first, event.Sequence, "duplicate deliveries must agree on the stored event")
	}

	require.Equal(t, 1, countStoredSourceEvents(ctx, t, runtime, scope.runID, sequence),
		"the source event must be stored exactly once")

	t.Logf("transaction replays observed: %d", atomic.LoadInt64(&replays))
}

// duplicateDeliveryOutcome is what concurrent duplicate deliveries produced:
// the sequence every caller saw, and the ones that failed.
type duplicateDeliveryOutcome struct {
	first  int64
	events []agentos.ConversationEvent
}

// deliverDuplicateEventConcurrently has writers deliver the same upstream event
// repeatedly at once, and fails the test on any error that escapes.
func deliverDuplicateEventConcurrently(
	ctx context.Context,
	t *testing.T,
	runtime *Runtime,
	scope *conversationTestScope,
	writers, deliveries int,
	sequence int64,
) duplicateDeliveryOutcome {
	t.Helper()

	var (
		waitGroup sync.WaitGroup
		events    = make(chan agentos.ConversationEvent, writers*deliveries)
		failures  = make(chan error, writers*deliveries)
	)

	for writer := range writers {
		waitGroup.Add(1)

		go deliverDuplicateEventRepeatedly(ctx, runtime, scope, writer, deliveries, sequence, events, failures, &waitGroup)
	}

	waitGroup.Wait()
	close(events)
	close(failures)

	outcome := duplicateDeliveryOutcome{}

	for err := range failures {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == pgSerializationFailure || pgErr.Code == pgUniqueViolation || pgErr.Code == pgDeadlockDetected) {
			t.Fatalf("a transaction abort reached the caller: %v", err)
		}

		t.Fatalf("duplicate delivery failed: %v", err)
	}

	for event := range events {
		if outcome.first == 0 {
			outcome.first = event.Sequence
		}

		outcome.events = append(outcome.events, event)
	}

	return outcome
}

// countStoredSourceEvents counts how many rows one source event occupies.
func countStoredSourceEvents(ctx context.Context, t *testing.T, runtime *Runtime, runID string, sequence int64) int {
	t.Helper()

	rows, err := runtime.pool.Query(ctx,
		`SELECT source_sequence FROM agentos_conversation_events WHERE run_id = $1 AND source_sequence = $2`,
		runID, sequence)
	if err != nil {
		t.Fatalf("read stored events: %v", err)
	}
	defer rows.Close()

	var stored int

	for rows.Next() {
		stored++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate stored events: %v", err)
	}

	return stored
}

// deliverDuplicateEventRepeatedly is one writer's share of the concurrent
// delivery: the same upstream event, repeated.
func deliverDuplicateEventRepeatedly(
	ctx context.Context,
	runtime *Runtime,
	scope *conversationTestScope,
	writer, deliveries int,
	sequence int64,
	events chan<- agentos.ConversationEvent,
	failures chan<- error,
	waitGroup *sync.WaitGroup,
) {
	defer waitGroup.Done()

	for range deliveries {
		event, err := runtime.IngestEvent(ctx, &agentos.ExternalConversationEvent{
			ThreadID: scope.threadID, RunID: scope.runID, ProcessID: scope.processID,
			AccountID: scope.accountID, ProjectID: scope.projectID,
			SourceEventID: conversationTestSourceID(scope.runID, sequence), SourceSequence: sequence,
			EventType: "kardcraft.node.started", OccurredAt: time.Now().UTC(),
			Payload: map[string]any{"writer": writer},
		})
		if err != nil {
			failures <- err

			continue
		}

		events <- event
	}
}
