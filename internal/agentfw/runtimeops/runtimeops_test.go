package runtimeops

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	errTestStreamDown = errors.New("stream down")
	errTestRetry      = errors.New("retry")
)

type stubPublisher struct {
	fail   bool
	events []StreamEvent
}

func (p *stubPublisher) Publish(_ context.Context, event *StreamEvent) error {
	if p.fail {
		return errTestStreamDown
	}

	p.events = append(p.events, *event)

	return nil
}

type flakyBillingSink struct {
	failFirst int
	calls     int
}

func (s *flakyBillingSink) Deliver(_ context.Context, _ *UsageRecord) error {
	s.calls++
	if s.calls <= s.failFirst {
		return errTestRetry
	}

	return nil
}

type memAuditSink struct {
	events []SecurityAuditEvent
}

func (s *memAuditSink) Write(_ context.Context, event SecurityAuditEvent) error {
	s.events = append(s.events, event)

	return nil
}

func TestSequencerAndEventSchemaVersion(t *testing.T) {
	t.Parallel()

	seq := NewSequencer()
	require.Equal(t, int64(1), seq.Next("run-1"))
	require.Equal(t, int64(2), seq.Next("run-1"))

	pub := &stubPublisher{}
	event := &StreamEvent{
		EventSchemaVersion: "v1",
		RunID:              "run-1",
		Sequence:           seq.Next("run-1"),
		Timestamp:          time.Now().UTC(),
		Type:               EventTypeStatus,
	}
	require.NoError(t, pub.Publish(context.Background(), event))
	require.Equal(t, "v1", event.EventSchemaVersion)

	ok, err := IsNMinusOneCompatible("v2", "v1")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = IsNMinusOneCompatible("v3", "v1")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestFailOpenPublisher(t *testing.T) {
	t.Parallel()

	inner := &stubPublisher{fail: true}
	called := false
	pub := FailOpenPublisher{
		Inner: inner,
		OnPublishError: func(err error) {
			called = err != nil
		},
	}
	require.NoError(t, pub.Publish(context.Background(), &StreamEvent{RunID: "run-2"}))
	require.True(t, called)
}

func TestAdmissionAcquireRelease(t *testing.T) {
	t.Parallel()

	ctrl := NewAdmissionController(map[string]int{"acc-1": 1})
	require.NoError(t, ctrl.Acquire("acc-1", false))
	require.ErrorIs(t, ctrl.Acquire("acc-1", false), ErrAdmissionLimitExceeded)
	ctrl.Release("acc-1")
	require.NoError(t, ctrl.Acquire("acc-1", false))
}

func TestUsageEmitterRetry(t *testing.T) {
	t.Parallel()

	sink := &flakyBillingSink{failFirst: 1}
	emitter := UsageEmitter{
		Sink:        sink,
		MaxAttempts: 3,
		Backoff:     time.Millisecond,
	}
	err := emitter.Emit(context.Background(), &UsageRecord{RunID: "run-3", ModelID: "gpt"})
	require.NoError(t, err)
	require.Equal(t, 2, sink.calls)
}

func TestObservabilityAndCorrelation(t *testing.T) {
	t.Parallel()

	rec := NewMetricsRecorder()
	rec.Inc("run_started")
	rec.Observe("step_latency", 20*time.Millisecond)
	require.Equal(t, int64(1), rec.Counter("run_started"))

	ctx := WithCorrelationID(context.Background(), "corr-1")
	require.Equal(t, "corr-1", CorrelationID(ctx))
}

func TestErrorPolicyTaxonomy(t *testing.T) {
	t.Parallel()

	require.True(t, PolicyForCategory(ProviderError).Retryable)
	require.False(t, PolicyForCategory(UserError).Retryable)
	require.False(t, PolicyForCategory(DeterminismError).Billable)
}

func TestSecurityAuditRedaction(t *testing.T) {
	t.Parallel()

	sink := &memAuditSink{}
	auditor := SecurityAuditor{
		Redactor: DefaultRedactionPolicy{},
		Sink:     sink,
	}

	err := auditor.Emit(context.Background(), SecurityAuditEvent{
		RunID:   "run-4",
		ActorID: "acc-1",
		Action:  "tool.authorize",
		Payload: map[string]any{
			"tool":  "payments",
			"token": "secret-token",
		},
	})
	require.NoError(t, err)
	require.Len(t, sink.events, 1)
	require.Equal(t, "[REDACTED]", sink.events[0].Payload["token"])
}

func TestRunIdentityPropagation(t *testing.T) {
	t.Parallel()

	id := RunIdentity{
		RunID:          "run-5",
		WorkflowID:     "wf-5",
		ThreadRunID:    "thread-run-5",
		IdempotencyKey: "idem-5",
	}
	require.NoError(t, ValidateRunIdentity(id))

	event := StreamEvent{
		EventSchemaVersion: "v1",
		Type:               EventTypeStatus,
	}
	require.NoError(t, AttachIdentityToEvent(&event, id))
	require.Equal(t, "run-5", event.RunID)
	require.Equal(t, "wf-5", event.Payload["workflow_id"])

	record := UsageRecord{}
	require.NoError(t, AttachIdentityToUsage(&record, id))
	require.Equal(t, "run-5", record.RunID)
	require.Equal(t, "thread-run-5", record.ThreadRunID)
	require.Equal(t, "idem-5", record.IdempotencyKey)
}
