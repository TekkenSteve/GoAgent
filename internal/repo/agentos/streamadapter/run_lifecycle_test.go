package streamadapter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
	"github.com/stretchr/testify/require"
)

// TestRunLifecyclePublishesStartedThenTerminal drives a one-shot backend's
// full observable lifecycle: Start publishes RUN_STARTED, a terminal Status
// publishes RUN_FINISHED exactly once, and a repeated terminal Status is a
// no-op.
func TestRunLifecyclePublishesStartedThenTerminal(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	lifecycle := NewRunLifecycle(bus, nil)
	sub := lifecycleSubscribe(t, bus, HandleForRun("acme", "run-1"))

	spec := &agentos.RunSpec{RunID: "run-1", ThreadID: "thread-1", AccountID: "acme"}
	ctx := context.Background()

	lifecycle.PublishStarted(ctx, spec, &agentos.RunStatus{LifecycleState: "running"})
	lifecycle.PublishStatus(ctx, "run-1", &agentos.RunStatus{LifecycleState: "completed"})
	lifecycle.PublishStatus(ctx, "run-1", &agentos.RunStatus{LifecycleState: "completed"})

	requireLifecycleType(t, sub, stream.EventRunStarted)
	requireLifecycleType(t, sub, stream.EventRunFinished)
	requireLifecycleQuiet(t, sub)
}

// TestRunLifecycleTerminalSpellings locks the normalization of backend
// lifecycle strings onto the three-event AG-UI terminal vocabulary.
func TestRunLifecycleTerminalSpellings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		state string
		want  stream.EventType
	}{
		{state: "completed", want: stream.EventRunFinished},
		{state: "succeeded", want: stream.EventRunFinished},
		{state: "failed", want: stream.EventRunError},
		{state: "canceled", want: stream.EventRunCancelled},
		{state: "cancelled", want: stream.EventRunCancelled},
	}

	for i := range cases {
		tc := cases[i]

		t.Run(tc.state, func(t *testing.T) {
			t.Parallel()

			bus := memstream.New()
			lifecycle := NewRunLifecycle(bus, nil)
			sub := lifecycleSubscribe(t, bus, HandleForRun("", "run-1"))

			spec := &agentos.RunSpec{RunID: "run-1", ThreadID: "thread-1"}
			lifecycle.PublishStarted(t.Context(), spec, &agentos.RunStatus{LifecycleState: "running"})
			lifecycle.PublishStatus(t.Context(), "run-1", &agentos.RunStatus{LifecycleState: tc.state})

			requireLifecycleType(t, sub, stream.EventRunStarted)
			requireLifecycleType(t, sub, tc.want)
			requireLifecycleQuiet(t, sub)
		})
	}
}

// TestRunLifecycleRunErrorCarriesReason ensures the remote's failure reason
// reaches the RUN_ERROR payload for the projector and frontend.
func TestRunLifecycleRunErrorCarriesReason(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	lifecycle := NewRunLifecycle(bus, nil)
	sub := lifecycleSubscribe(t, bus, HandleForRun("", "run-1"))

	lifecycle.PublishStarted(t.Context(), &agentos.RunSpec{RunID: "run-1"}, &agentos.RunStatus{LifecycleState: "running"})
	lifecycle.PublishStatus(t.Context(), "run-1", &agentos.RunStatus{LifecycleState: "failed", Reason: "boom"})

	requireLifecycleType(t, sub, stream.EventRunStarted)
	stored := requireLifecycleEvent(t, sub)
	require.Equal(t, stream.EventRunError, stored.Event.Type)
	require.Equal(t, map[string]any{"message": "run failed: boom"}, stored.Event.Payload[stream.FieldError])
}

// TestRunLifecycleTerminalAtStart covers a Start response that already reports
// a terminal state: the adapter opens and closes the timeline in one call.
func TestRunLifecycleTerminalAtStart(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	lifecycle := NewRunLifecycle(bus, nil)
	sub := lifecycleSubscribe(t, bus, HandleForRun("", "run-1"))

	lifecycle.PublishStarted(t.Context(), &agentos.RunSpec{RunID: "run-1"}, &agentos.RunStatus{LifecycleState: "completed"})

	requireLifecycleType(t, sub, stream.EventRunStarted)
	requireLifecycleType(t, sub, stream.EventRunFinished)
	requireLifecycleQuiet(t, sub)
}

// TestRunLifecycleNonTerminalStatusIsNoOp asserts intermediate states never
// leak onto the timeline.
func TestRunLifecycleNonTerminalStatusIsNoOp(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	lifecycle := NewRunLifecycle(bus, nil)
	sub := lifecycleSubscribe(t, bus, HandleForRun("", "run-1"))

	lifecycle.PublishStarted(t.Context(), &agentos.RunSpec{RunID: "run-1"}, &agentos.RunStatus{LifecycleState: "running"})
	requireLifecycleType(t, sub, stream.EventRunStarted)

	lifecycle.PublishStatus(t.Context(), "run-1", &agentos.RunStatus{LifecycleState: "running"})
	lifecycle.PublishStatus(t.Context(), "run-1", &agentos.RunStatus{LifecycleState: "created"})
	requireLifecycleQuiet(t, sub)
}

// TestRunLifecycleUnknownRunIsNoOp guards against a Status observation for a
// run this adapter never saw Start publish anything.
func TestRunLifecycleUnknownRunIsNoOp(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	lifecycle := NewRunLifecycle(bus, nil)
	sub := lifecycleSubscribe(t, bus, HandleForRun("", "run-1"))

	lifecycle.PublishStatus(t.Context(), "ghost", &agentos.RunStatus{LifecycleState: "completed"})
	requireLifecycleQuiet(t, sub)
}

// TestRunLifecycleRejectsInvalidSpec guards the adapter against a nil or
// id-less spec without panicking.
func TestRunLifecycleRejectsInvalidSpec(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	lifecycle := NewRunLifecycle(bus, nil)
	sub := lifecycleSubscribe(t, bus, HandleForRun("", "run-1"))

	lifecycle.PublishStarted(t.Context(), nil, &agentos.RunStatus{})
	lifecycle.PublishStarted(t.Context(), &agentos.RunSpec{}, &agentos.RunStatus{})
	requireLifecycleQuiet(t, sub)
}

// TestRunLifecycleNilPublisherIsNoOp degrades gracefully when the runtime was
// built without a data plane: the run works, its milestones never reach a bus.
func TestRunLifecycleNilPublisherIsNoOp(t *testing.T) {
	t.Parallel()

	lifecycle := NewRunLifecycle(nil, nil)

	lifecycle.PublishStarted(t.Context(), &agentos.RunSpec{RunID: "run-1"}, &agentos.RunStatus{LifecycleState: "running"})
	lifecycle.PublishStatus(t.Context(), "run-1", &agentos.RunStatus{LifecycleState: "completed"})
}

// TestRunLifecycleEnsureFiresOncePerRun verifies the projection-attach hook is
// fired once per run with the run's identity and channel handle.
func TestRunLifecycleEnsureFiresOncePerRun(t *testing.T) {
	t.Parallel()

	bus := memstream.New()

	var ensured atomic.Int64

	lifecycle := NewRunLifecycle(bus, nil).WithEnsure(func(_ context.Context, runID string, handle *stream.Handle) error {
		ensured.Add(1)
		require.Equal(t, "run-1", runID)
		require.Equal(t, HandleForRun("acme", "run-1").Channel, handle.Channel)

		return nil
	})

	sub := lifecycleSubscribe(t, bus, HandleForRun("acme", "run-1"))
	spec := &agentos.RunSpec{RunID: "run-1", ThreadID: "thread-1", AccountID: "acme"}

	// Three publishes on the same run: the hook fires once.
	lifecycle.PublishStarted(t.Context(), spec, &agentos.RunStatus{LifecycleState: "running"})
	lifecycle.PublishStatus(t.Context(), "run-1", &agentos.RunStatus{LifecycleState: "completed"})
	lifecycle.PublishStatus(t.Context(), "run-1", &agentos.RunStatus{LifecycleState: "completed"})

	requireLifecycleType(t, sub, stream.EventRunStarted)
	requireLifecycleType(t, sub, stream.EventRunFinished)
	require.Equal(t, int64(1), ensured.Load())
}

func lifecycleSubscribe(t *testing.T, bus *memstream.Bus, handle *stream.Handle) *stream.Subscription {
	t.Helper()

	sub, err := bus.Subscribe(t.Context(), handle, 0)
	require.NoError(t, err)

	t.Cleanup(sub.Close)

	return sub
}

func requireLifecycleType(t *testing.T, sub *stream.Subscription, want stream.EventType) {
	t.Helper()

	select {
	case stored := <-sub.C:
		require.Equal(t, want, stored.Event.Type)
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", want)
	}
}

func requireLifecycleEvent(t *testing.T, sub *stream.Subscription) stream.StoredEvent {
	t.Helper()

	select {
	case stored := <-sub.C:
		return stored
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")

		return stream.StoredEvent{}
	}
}

// requireLifecycleQuiet asserts the subscription delivers nothing within the
// window — the terminal milestone was not duplicated.
func requireLifecycleQuiet(t *testing.T, sub *stream.Subscription) {
	t.Helper()

	select {
	case stored := <-sub.C:
		t.Fatalf("unexpected event %s after terminal", stored.Event.Type)
	case <-time.After(200 * time.Millisecond):
	}
}
