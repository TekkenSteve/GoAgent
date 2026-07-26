package conversation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestRuntimePostgresConversationLifecycle(t *testing.T) {
	t.Parallel()

	postgresURL := os.Getenv("AGENTOS_TEST_PG_URL")
	if postgresURL == "" {
		t.Skip("AGENTOS_TEST_PG_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	runtime := newTestRuntime(ctx, t, Config{PostgresURL: postgresURL, PollInterval: 10 * time.Millisecond})
	defer runtime.Close()

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

	cleanupConversationThread(t, runtime, threadID)

	snapshot := startConversationTestRun(ctx, t, runtime, &scope, suffix)

	subscription, err := runtime.SubscribeThread(ctx, agentos.ThreadStreamScope{
		ThreadID: threadID, AccountID: accountID, ProjectID: projectID, AfterSequence: snapshot.Cursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()

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

func TestRuntimeRedisStreamDeliversWithoutFallbackPoll(t *testing.T) {
	t.Parallel()

	postgresURL := os.Getenv("AGENTOS_TEST_PG_URL")

	redisURL := os.Getenv("AGENTOS_TEST_REDIS_URL")
	if postgresURL == "" || redisURL == "" {
		t.Skip("AGENTOS_TEST_PG_URL and AGENTOS_TEST_REDIS_URL are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runtime := newTestRuntime(ctx, t, Config{
		PostgresURL: postgresURL, RedisURL: redisURL,
		PollInterval: 30 * time.Second, OutboxPollInterval: 30 * time.Second,
	})
	defer runtime.Close()

	suffix := fmt.Sprintf("redis-%d", time.Now().UnixNano())
	threadID := "thread-" + suffix
	runID := "run-" + suffix
	accountID := "account-" + suffix
	projectID := "project-" + suffix
	scope := conversationTestScope{
		threadID: threadID, runID: runID, processID: "process-" + suffix,
		accountID: accountID, projectID: projectID,
	}

	cleanupConversationThread(t, runtime, threadID)
	cleanupConversationStream(t, runtime, accountID, projectID, threadID)

	subscription, started := startRedisStreamingRun(ctx, t, runtime, &scope, suffix)
	defer subscription.Close()

	writer := newTestRuntime(ctx, t, Config{PostgresURL: postgresURL, PollInterval: 30 * time.Second})
	defer writer.Close()

	gapEvents := writeGapEvents(ctx, t, writer, &scope, suffix)

	if err := runtime.stream.Publish(ctx, &gapEvents[1], accountID, projectID); err != nil {
		t.Fatal(err)
	}

	assertSubscriptionSequenceBefore(t, subscription, 5, 6, 2*time.Second)

	if err := runtime.stream.Publish(ctx, &started, accountID, projectID); err != nil {
		t.Fatal(err)
	}

	assertNoConversationEvent(t, subscription, 150*time.Millisecond)
}

func startRedisStreamingRun(ctx context.Context, t *testing.T, runtime *Runtime, scope *conversationTestScope, suffix string) (core.Subscription, agentos.ConversationEvent) {
	t.Helper()

	_, err := runtime.StartRun(ctx, &agentos.StartConversationRunSpec{
		RunID: scope.runID, ThreadID: scope.threadID, ProcessID: scope.processID,
		AccountID: scope.accountID, ProjectID: scope.projectID, MessageID: "message-" + suffix,
		UserMessage: "Question", IdempotencyKey: "request-" + suffix, RequestedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}

	subscription, err := runtime.SubscribeThread(ctx, agentos.ThreadStreamScope{
		ThreadID: scope.threadID, AccountID: scope.accountID, ProjectID: scope.projectID, AfterSequence: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	startedAt := time.Now()
	started := scope.ingest(ctx, t, runtime, 1, agentos.ConversationEventRunStarted, map[string]any{})
	assertRedisLiveEvent(t, subscription, startedAt)

	return subscription, started
}

func assertRedisLiveEvent(t *testing.T, subscription core.Subscription, startedAt time.Time) {
	t.Helper()

	select {
	case event := <-subscription.Events():
		if event.Sequence != 4 || event.EventType != agentos.ConversationEventRunStarted {
			t.Fatalf("live event = %#v", event)
		}

		if elapsed := time.Since(startedAt); elapsed >= 2*time.Second {
			t.Fatalf("redis live delivery took %s; fallback poll is 30s", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("redis live event did not arrive before the fallback poll")
	}
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

func assertNoConversationEvent(t *testing.T, subscription core.Subscription, timeout time.Duration) {
	t.Helper()

	select {
	case duplicate := <-subscription.Events():
		t.Fatalf("duplicate redis event was delivered: %#v", duplicate)
	case <-time.After(timeout):
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

func cleanupConversationThread(t *testing.T, runtime *Runtime, threadID string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := runtime.pool.Exec(context.Background(), `DELETE FROM agentos_threads WHERE thread_id = $1`, threadID); err != nil {
			t.Errorf("delete conversation thread: %v", err)
		}
	})
}

func cleanupConversationStream(t *testing.T, runtime *Runtime, accountID, projectID, threadID string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := runtime.stream.rdb.Del(context.Background(), conversationStreamKey(accountID, projectID, threadID)); err != nil {
			t.Errorf("delete conversation stream: %v", err)
		}
	})
}
