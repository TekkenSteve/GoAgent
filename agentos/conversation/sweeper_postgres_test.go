package conversation

import (
	"context"
	"fmt"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/stretchr/testify/require"
)

// A running run holds the thread: its partial unique index is the thread's turn
// lock. Two ways that lock outlives its owner are covered here, and both end
// with the thread usable again — one because the sweeper abandons a run that
// stopped existing, one because it starts the turn that was waiting.

// newSweeperTestRuntime builds a runtime whose sweeper runs only when the test
// asks: the background loop's interval is long enough not to fire.
func newSweeperTestRuntime(ctx context.Context, t *testing.T, postgresURL string, lease time.Duration) *Runtime {
	t.Helper()

	runtime := newTestRuntime(ctx, t, Config{
		PostgresURL:        postgresURL,
		PollInterval:       10 * time.Millisecond,
		RunLease:           lease,
		LeaseSweepInterval: time.Hour,
	})
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	return runtime
}

func TestRuntimeAbandonsExpiredRunAndReleasesTheThread(t *testing.T) {
	t.Parallel()

	postgresURL := newConversationTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// A lease this short is not a deployment setting; it is how the test gets
	// to the state a deployment reaches after half an hour of silence.
	runtime := newSweeperTestRuntime(ctx, t, postgresURL, 20*time.Millisecond)

	suffix := fmt.Sprintf("abandon-%d", time.Now().UnixNano())
	scope := conversationTestScope{
		threadID: "thread-" + suffix, runID: "run-" + suffix, processID: "process-" + suffix,
		accountID: "account-" + suffix, projectID: "project-" + suffix,
	}

	startConversationTestRun(ctx, t, runtime, &scope, suffix)
	scope.ingest(ctx, t, runtime, 1, agentos.ConversationEventRunStarted, map[string]any{})
	require.Equal(t, agentos.ConversationRunRunning, runStatus(ctx, t, runtime, scope.runID))

	// The backend dies here: no RUN_FINISHED will ever arrive.
	time.Sleep(30 * time.Millisecond)

	abandoned, _, err := runtime.sweepOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, abandoned, "the run that stopped producing facts must be abandoned")

	run := loadRun(ctx, t, runtime, scope.runID)
	require.Equal(t, agentos.ConversationRunError, run.Status)
	require.Equal(t, agentos.ConversationOutcomeAbandoned, run.Outcome)
	require.Equal(t, "lease_expired", run.ErrorCode)
	require.NotEmpty(t, run.Error)

	// The abandonment is a fact in the thread's history, with its reason: an
	// operator reading the conversation can tell why the run ended.
	abandonment := loadEventBySource(ctx, t, runtime, scope.threadID, abandonedSourceEventID(scope.runID))

	require.Equal(t, agentos.ConversationEventRunFinished, abandonment.EventType)
	require.Equal(t, agentos.ConversationOutcomeAbandoned, stringPayload(abandonment.Payload, "outcome"))
	require.Equal(t, "lease_expired", stringPayload(abandonment.Payload, "termination_reason"))

	// The thread is usable again: the next turn starts where the dead one left
	// the lock.
	// A distinct idempotency key: a new turn, not a retry of the one that was
	// abandoned.
	nextScope := &conversationTestScope{
		threadID: scope.threadID, runID: "next-" + suffix, processID: scope.processID,
		accountID: scope.accountID, projectID: scope.projectID,
	}

	// Started directly rather than through the fresh-thread helper: this thread
	// already has history, and what matters here is that its next turn is
	// admitted at all.
	nextRun, err := runtime.StartRun(ctx, &agentos.StartConversationRunSpec{
		RunID: nextScope.runID, ThreadID: scope.threadID, ProcessID: scope.processID,
		AccountID: scope.accountID, ProjectID: scope.projectID,
		MessageID: "next-user-" + suffix, UserMessage: "Again",
		IdempotencyKey: "next-request-" + suffix, RequestedAt: time.Now().UTC(),
	})
	require.NoError(t, err, "the thread must accept a new turn once the dead run released it")
	require.Equal(t, agentos.ConversationRunPending, nextRun.Status)

	nextScope.ingest(ctx, t, runtime, 1, agentos.ConversationEventRunStarted, map[string]any{})
	require.Equal(t, agentos.ConversationRunRunning, runStatus(ctx, t, runtime, nextScope.runID))
}

// A sweep must not abandon a run that is still producing facts, however long
// it has been running.
func TestRuntimeKeepsActiveRuns(t *testing.T) {
	t.Parallel()

	postgresURL := newConversationTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	runtime := newSweeperTestRuntime(ctx, t, postgresURL, 30*time.Millisecond)

	suffix := fmt.Sprintf("active-%d", time.Now().UnixNano())
	scope := conversationTestScope{
		threadID: "thread-" + suffix, runID: "run-" + suffix, processID: "process-" + suffix,
		accountID: "account-" + suffix, projectID: "project-" + suffix,
	}

	startConversationTestRun(ctx, t, runtime, &scope, suffix)
	scope.ingest(ctx, t, runtime, 1, agentos.ConversationEventRunStarted, map[string]any{})

	// Facts keep arriving, so the lease never expires: the run is slow, not
	// dead, and abandoning it would cut real work short.
	for sequence := int64(2); sequence <= 5; sequence++ {
		time.Sleep(15 * time.Millisecond)

		scope.ingest(ctx, t, runtime, sequence, agentos.ConversationEventTextMessageStart, map[string]any{
			"message_id": fmt.Sprintf("message-%d-%s", sequence, suffix), "role": "assistant",
		})
	}

	abandoned, _, err := runtime.sweepOnce(ctx)
	require.NoError(t, err)
	require.Zero(t, abandoned, "a run that keeps producing facts must survive")

	require.Equal(t, agentos.ConversationRunRunning, runStatus(ctx, t, runtime, scope.runID))
}

// A steer turn's RUN_STARTED cannot take the thread while its predecessor
// streams. It used to be refused, which lost the turn: the producer delivered
// it once. It is now parked and applied when the predecessor finishes.
func TestRuntimeParksSteerTurnUntilPredecessorFinishes(t *testing.T) {
	t.Parallel()

	postgresURL := newConversationTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	runtime := newSweeperTestRuntime(ctx, t, postgresURL, time.Hour)

	suffix := fmt.Sprintf("steer-%d", time.Now().UnixNano())
	first := conversationTestScope{
		threadID: "thread-" + suffix, runID: "first-" + suffix, processID: "process-" + suffix,
		accountID: "account-" + suffix, projectID: "project-" + suffix,
	}

	startConversationTestRun(ctx, t, runtime, &first, suffix)
	first.ingest(ctx, t, runtime, 1, agentos.ConversationEventRunStarted, map[string]any{})
	require.Equal(t, agentos.ConversationRunRunning, runStatus(ctx, t, runtime, first.runID))

	// The steer turn is admitted while the first run streams, and its
	// RUN_STARTED must not be refused.
	steer := &conversationTestScope{
		threadID: first.threadID, runID: "steer-" + suffix, processID: first.processID,
		accountID: first.accountID, projectID: first.projectID,
	}

	_, err := runtime.StartRun(ctx, &agentos.StartConversationRunSpec{
		RunID: steer.runID, ThreadID: first.threadID, ProcessID: first.processID,
		AccountID: first.accountID, ProjectID: first.projectID,
		MessageID: "steer-user-" + suffix, UserMessage: "Correction",
		IdempotencyKey: "steer-request-" + suffix, RequestedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	// The caller is told the fact is durable, not that its turn was lost.
	_, err = runtime.IngestEvent(ctx, &agentos.ExternalConversationEvent{
		ThreadID: first.threadID, RunID: steer.runID, ProcessID: first.processID,
		AccountID: first.accountID, ProjectID: first.projectID,
		SourceEventID: "steer-start-" + suffix, SourceSequence: 1,
		EventType: agentos.ConversationEventRunStarted, OccurredAt: time.Now().UTC(),
		Payload: map[string]any{},
	})
	require.NoError(t, err, "a steer turn's start must be accepted while the predecessor streams")

	require.Equal(t, agentos.ConversationRunPending, runStatus(ctx, t, runtime, steer.runID),
		"the parked turn waits for the thread")

	// A sweep with no free thread changes nothing.
	_, projected, err := runtime.sweepOnce(ctx)
	require.NoError(t, err)
	require.Zero(t, projected, "a thread that is still taken has nothing to start")

	// The predecessor finishes, and the same sweeper starts the parked turn.
	first.ingest(ctx, t, runtime, 2, agentos.ConversationEventRunFinished, map[string]any{
		"outcome": agentos.ConversationOutcomeNormal,
	})

	requireSweepStartsParkedTurn(ctx, t, runtime, &first, steer)
}

// requireSweepStartsParkedTurn drives the one sweep that must free the thread
// and asserts that both turns are on the timeline afterwards.
func requireSweepStartsParkedTurn(
	ctx context.Context,
	t *testing.T,
	runtime *Runtime,
	first *conversationTestScope,
	steer *conversationTestScope,
) {
	t.Helper()

	_, projected, err := runtime.sweepOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, projected, "the parked turn must start once the thread is free")

	require.Equal(t, agentos.ConversationRunRunning, runStatus(ctx, t, runtime, steer.runID))

	// Both turns are on the timeline: nothing was dropped.
	snapshot, err := runtime.GetThreadSnapshot(ctx, agentos.ThreadScope{
		ThreadID: first.threadID, AccountID: first.accountID, ProjectID: first.projectID,
	})
	require.NoError(t, err)

	require.Len(t, snapshot.Runs, 2)
	require.Equal(t, agentos.ConversationRunCompleted, statusOfRun(snapshot.Runs, first.runID))
	require.Equal(t, agentos.ConversationRunRunning, statusOfRun(snapshot.Runs, steer.runID))
}

// runStatus reads one run's status straight from the runtime.
func runStatus(ctx context.Context, t *testing.T, runtime *Runtime, runID string) string {
	t.Helper()

	return loadRun(ctx, t, runtime, runID).Status
}

// loadRun reads a run row as the API would see it.
func loadRun(ctx context.Context, t *testing.T, runtime *Runtime, runID string) agentos.ConversationRun {
	t.Helper()

	rows, err := runtime.pool.Query(ctx, `
		SELECT run_id, thread_id, process_id, account_id, project_id, status, outcome,
		       error_code, error_message, created_at, started_at, completed_at
		FROM agentos_conversation_runs WHERE run_id = $1`, runID)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("run %s not found", runID)
	}

	var (
		run         agentos.ConversationRun
		startedAt   *time.Time
		completedAt *time.Time
	)

	if err := rows.Scan(&run.RunID, &run.ThreadID, &run.ProcessID, &run.AccountID, &run.ProjectID,
		&run.Status, &run.Outcome, &run.ErrorCode, &run.Error, &run.CreatedAt, &startedAt, &completedAt); err != nil {
		t.Fatalf("scan run: %v", err)
	}

	// A run that has not started or finished has no timestamps, and the public
	// shape carries them as values.
	if startedAt != nil {
		run.StartedAt = *startedAt
	}

	if completedAt != nil {
		run.CompletedAt = *completedAt
	}

	return run
}

// loadEventBySource reads the fact with the given source event id: the
// deterministic identity the sweeper writes, so the test asserts the fact it
// means rather than whichever event happens to come next.
func loadEventBySource(ctx context.Context, t *testing.T, runtime *Runtime, threadID, sourceEventID string) agentos.ConversationEvent {
	t.Helper()

	rows, err := runtime.pool.Query(ctx, `
		SELECT event_id, thread_id, run_id, process_id, sequence, source_event_id,
		       source_sequence, event_type, occurred_at, payload
		FROM agentos_conversation_events
		WHERE thread_id = $1 AND source_event_id = $2`, threadID, sourceEventID)
	if err != nil {
		t.Fatalf("load event: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("event %s not found on thread %s", sourceEventID, threadID)
	}

	event, err := scanEvent(rows)
	if err != nil {
		t.Fatalf("scan event: %v", err)
	}

	return event
}

func statusOfRun(runs []agentos.ConversationRun, runID string) string {
	for i := range runs {
		if runs[i].RunID == runID {
			return runs[i].Status
		}
	}

	return ""
}
