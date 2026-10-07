package dshbackend

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
	"github.com/stretchr/testify/require"
)

// Run1 is the run id the bus assertions subscribe and assert on.
const Run1 = "run-1"

// Shared run scope for the bus assertions.
const (
	testAccountID = "acme"
	testThreadID  = "thread-1"
)

// newTestBackend wires a dsh backend against the fake dsh SDK subprocess.
// The subscriber may be nil (Subscribe then fails); the publisher is a fresh
// memstream bus so the test can assert the run's timeline.
func newTestBackend(t *testing.T, probe *agentosruntimetest.SubscriberProbe, bus *memstream.Bus, script string) *Backend {
	t.Helper()

	backend, err := NewBackend(probe, bus, nil, nil, &Config{
		Name:    fakeBackendName,
		Command: testExecutable(t),
		Args:    []string{"-test.run=TestDSHFakeHelper"},
		Env: []string{
			envDSHFakeHelper + "=1",
			envDSHFakeScript + "=" + script,
		},
	})
	require.NoError(t, err)

	// withDefaults fills Profile with "sdk"; the fake test binary must not see
	// --profile, so the test clears it before the first spawn.
	backend.config.Profile = ""

	t.Cleanup(func() {
		require.NoError(t, backend.Close())
	})

	return backend
}

// newTestBackendWithFacts wires the same fake subprocess with a durable-fact
// recorder, so a test can make fact writes fail and assert what the adapter
// does about it. Nil facts keeps the pure mirror behavior.
func newTestBackendWithFacts(t *testing.T, bus *memstream.Bus, script string, facts *streamadapter.MilestoneRecorder) *Backend {
	t.Helper()

	backend, err := NewBackend(nil, bus, facts, nil, &Config{
		Name:    fakeBackendName,
		Command: testExecutable(t),
		Args:    []string{"-test.run=TestDSHFakeHelper"},
		Env: []string{
			envDSHFakeHelper + "=1",
			envDSHFakeScript + "=" + script,
		},
	})
	require.NoError(t, err)

	backend.config.Profile = ""

	t.Cleanup(func() {
		require.NoError(t, backend.Close())
	})

	return backend
}

// failingRunEventStore is a run-event store whose writes fail after the first
// succeedFor calls, so a test can fail a specific milestone.
type failingRunEventStore struct {
	succeedFor int
	calls      int
}

func (s *failingRunEventStore) LastRunEventSequence(context.Context, string) (int64, error) {
	return 0, nil
}

func (s *failingRunEventStore) AppendRunEvent(_ context.Context, ev *agentoscore.Event) error {
	s.calls++

	if s.calls > s.succeedFor {
		return fmt.Errorf("%w: %s", errTestFactStoreDown, ev.EventType)
	}

	return nil
}

// errTestFactStoreDown stands in for a Postgres that cannot take the write.
var errTestFactStoreDown = errors.New("test fact store is down")

// subscribeRun opens a live bus subscription on a run's channel and closes it
// with the test.
func subscribeRun(t *testing.T, bus *memstream.Bus, handle *stream.Handle) *stream.Subscription {
	t.Helper()

	sub, err := bus.Subscribe(t.Context(), handle, 0)
	require.NoError(t, err)

	t.Cleanup(sub.Close)

	return sub
}

// collectBusEvents drains a subscription until the terminal event lands.
func collectBusEvents(t *testing.T, sub *stream.Subscription, terminal stream.EventType) []stream.StoredEvent {
	t.Helper()

	var got []stream.StoredEvent

	for {
		select {
		case stored, ok := <-sub.C:
			if !ok {
				t.Fatalf("subscription closed before %s", terminal)
			}

			got = append(got, stored)

			if stored.Event.Type == terminal {
				return got
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %s; got %d events", terminal, len(got))
		}
	}
}

// typesOf extracts the event types of a stored-event batch.
func typesOf(stored []stream.StoredEvent) []stream.EventType {
	types := make([]stream.EventType, 0, len(stored))

	for i := range stored {
		types = append(types, stored[i].Event.Type)
	}

	return types
}

func TestBackendConformance(t *testing.T) {
	t.Parallel()

	probe := &agentosruntimetest.SubscriberProbe{}

	backend := newTestBackend(t, probe, memstream.New(), fakeScriptMinimal)

	agentosruntimetest.RunBackendConformance(t, &agentosruntimetest.BackendConformanceCase{
		Name:            "dsh",
		Backend:         backend,
		Ref:             backend.config.Ref(),
		StatusState:     "running",
		SubscriberProbe: probe,
	})
}

// TestBackendStreamsToBus is the acceptance demo for the dsh backend: a real
// fake-dsh subprocess, the real mapper, and a real bus. Start publishes
// RUN_STARTED; the scripted turn flows as text deltas (paired START/END),
// a tool call arc (with the name learned from tool/call), and a completed
// turn/end closes with RUN_FINISHED — the stream reaches the bus, never a
// frontend directly.
func TestBackendStreamsToBus(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	backend := newTestBackend(t, nil, bus, fakeScriptStream)
	ctx := t.Context()

	sub := subscribeRun(t, bus, streamadapter.HandleForRun(testAccountID, Run1))

	spec := agentos.RunSpec{
		RunID:       Run1,
		ThreadID:    testThreadID,
		AccountID:   testAccountID,
		Backend:     backend.config.Ref(),
		UserMessage: testPromptText,
	}

	_, err := backend.Start(ctx, &spec)
	require.NoError(t, err)

	got := collectBusEvents(t, sub, stream.EventRunFinished)
	requireStreamShape(t, got)
}

// requireStreamShape asserts the exact run timeline the fake stream script
// produces: event types in order, one synthesized text message closed at the
// tool call, the tool arc with the name learned from tool/call, a stop finish
// reason, strictly increasing sequences, and exactly one terminal.
func requireStreamShape(t *testing.T, got []stream.StoredEvent) {
	t.Helper()

	require.Equal(t, []stream.EventType{
		stream.EventRunStarted,
		stream.EventTextMessageStart,
		stream.EventTextMessageContent,
		stream.EventTextMessageContent,
		stream.EventTextMessageEnd,
		stream.EventToolCallStart,
		stream.EventToolCallEnd,
		stream.EventCustom,
		stream.EventToolCallResult,
		stream.EventRunFinished,
	}, typesOf(got))

	// The two text deltas share one synthesized message, closed at the tool call.
	textStart := got[1].Event
	textEnd := got[4].Event

	require.Equal(t, "m-1", textStart.MessageID)
	require.Equal(t, textStart.MessageID, textEnd.MessageID)

	// The tool arc carries the call id and the name learned from tool/call.
	toolCall := got[5].Event
	require.Equal(t, testCallID, toolCall.Payload[stream.FieldCallID])
	require.Equal(t, testToolName, toolCall.Payload[stream.FieldName])

	execStart := got[7].Event
	require.Equal(t, "agentos.tool.execution.start", execStart.Payload[stream.FieldName])
	require.Equal(t, testToolName, execStart.Payload["toolName"])

	toolResult := got[8].Event
	require.Equal(t, testToolOutput, toolResult.Payload[stream.FieldResult])

	require.Equal(t, "stop", got[9].Event.Payload["finishReason"])

	// Sequences are strictly increasing and exactly one terminal lands.
	for i := 1; i < len(got); i++ {
		require.Greater(t, got[i].Sequence, got[i-1].Sequence)
	}

	terminalTypes := map[stream.EventType]bool{
		stream.EventRunFinished: true,
		stream.EventRunError:    true,
		stream.EventRunCanceled: true,
	}

	terminals := 0

	for i := range got {
		if terminalTypes[got[i].Event.Type] {
			terminals++
		}
	}

	require.Equal(t, 1, terminals)
}

// TestBackendCancelPublishesCanceled verifies the adapter-level cancel: the
// cancel flag is honored at the next turn/end, publishing RUN_CANCELED and
// flipping Status to canceled. The fake emits the turn/end only on shutdown,
// so the cancel always lands before it.
func TestBackendCancelPublishesCanceled(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	backend := newTestBackend(t, nil, bus, fakeScriptCancel)
	ctx := t.Context()

	sub := subscribeRun(t, bus, streamadapter.HandleForRun(testAccountID, Run1))

	spec := agentos.RunSpec{
		RunID:       Run1,
		ThreadID:    testThreadID,
		AccountID:   testAccountID,
		Backend:     backend.config.Ref(),
		UserMessage: testPromptText,
	}

	_, err := backend.Start(ctx, &spec)
	require.NoError(t, err)

	// Cancel must not flip Status synchronously (conformance contract).
	require.NoError(t, backend.Control(ctx, Run1, &agentoscore.ControlRequest{Operation: agentoscore.ControlCancel}))

	status, err := backend.Status(ctx, Run1)
	require.NoError(t, err)
	require.Equal(t, stateRunning, status.LifecycleState)

	// Shutdown makes the fake emit the turn/end, which maps to RUN_CANCELED.
	require.NoError(t, backend.Close())

	got := collectBusEvents(t, sub, stream.EventRunCanceled)
	require.Equal(t, []stream.EventType{stream.EventRunStarted, stream.EventRunCanceled}, typesOf(got))

	status, err = backend.Status(ctx, Run1)
	require.NoError(t, err)
	require.Equal(t, stateCanceled, status.LifecycleState)
}

func TestBackendRejectsNilInputs(t *testing.T) {
	t.Parallel()

	backend := &Backend{config: Config{Name: fakeBackendName}}

	if _, err := backend.Start(context.Background(), nil); !errors.Is(err, agentoscore.ErrInvalidRunSpec) {
		t.Fatalf("Start nil error = %v, want ErrInvalidRunSpec", err)
	}

	if err := backend.Signal(context.Background(), Run1, nil); !errors.Is(err, agentoscore.ErrInvalidSignal) {
		t.Fatalf("Signal nil error = %v, want ErrInvalidSignal", err)
	}

	if err := backend.Control(context.Background(), Run1, nil); !errors.Is(err, agentoscore.ErrInvalidControlOperation) {
		t.Fatalf("Control nil error = %v, want ErrInvalidControlOperation", err)
	}
}

func TestBackendNewBackendRequiresPublisher(t *testing.T) {
	t.Parallel()

	_, err := NewBackend(nil, nil, nil, nil, &Config{Name: fakeBackendName})
	require.ErrorIs(t, err, agentoscore.ErrInvalidBackendRef)
}

func TestBackendRejectsWrongBackendRef(t *testing.T) {
	t.Parallel()

	backend, err := NewBackend(nil, memstream.New(), nil, nil, &Config{Name: fakeBackendName})
	require.NoError(t, err)

	spec := agentos.RunSpec{RunID: Run1, Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "other"}}

	_, err = backend.Start(context.Background(), &spec)
	require.ErrorIs(t, err, agentoscore.ErrInvalidBackendRef)
}

func TestBackendSignalUnknownRun(t *testing.T) {
	t.Parallel()

	backend := &Backend{config: Config{Name: fakeBackendName}}

	signal := agentoscore.Signal{Type: agentoscore.SignalUserMessage}
	require.ErrorIs(t, backend.Signal(context.Background(), Run1, &signal), errDSHRunNotFound)
}

func TestBackendControlUnknownRun(t *testing.T) {
	t.Parallel()

	backend := &Backend{config: Config{Name: fakeBackendName}}

	control := agentoscore.ControlRequest{Operation: agentoscore.ControlCancel}
	require.ErrorIs(t, backend.Control(context.Background(), Run1, &control), errDSHRunNotFound)
}

func TestBackendStatusUnknownRun(t *testing.T) {
	t.Parallel()

	backend := &Backend{config: Config{Name: fakeBackendName}}

	_, err := backend.Status(context.Background(), Run1)
	require.ErrorIs(t, err, errDSHRunNotFound)
}

func TestBackendSubscribeRequiresSubscriber(t *testing.T) {
	t.Parallel()

	backend := &Backend{config: Config{Name: fakeBackendName}}

	_, err := backend.Subscribe(context.Background(), agentoscore.StreamScope{RunID: Run1})
	require.ErrorIs(t, err, errDSHSubscriberNotConfigured)
}

// TestBackendStartFailsWhenRunStartCannotPersist locks the rule that a run whose
// first fact never landed does not start: reporting it running would announce a
// timeline the durable copy does not have.
func TestBackendStartFailsWhenRunStartCannotPersist(t *testing.T) {
	t.Parallel()

	store := &failingRunEventStore{}
	facts, err := streamadapter.NewMilestoneRecorder(store, nil)
	require.NoError(t, err)

	backend := newTestBackendWithFacts(t, memstream.New(), fakeScriptStream, facts)

	spec := agentos.RunSpec{
		RunID:       Run1,
		ThreadID:    testThreadID,
		AccountID:   testAccountID,
		Backend:     backend.config.Ref(),
		UserMessage: testPromptText,
	}

	_, err = backend.Start(t.Context(), &spec)
	require.ErrorIs(t, err, errTestFactStoreDown)

	status, statusErr := backend.Status(t.Context(), Run1)
	require.NoError(t, statusErr)
	require.Equal(t, stateFailed, status.LifecycleState)
}

// TestBackendFailsRunWhenMilestoneCannotPersist locks the mid-run half: once a
// milestone cannot be persisted the consume loop stops and the run ends failed,
// instead of streaming a timeline whose durable copy has a hole.
func TestBackendFailsRunWhenMilestoneCannotPersist(t *testing.T) {
	t.Parallel()

	// RUN_STARTED lands, the first text milestone does not.
	store := &failingRunEventStore{succeedFor: 1}
	facts, err := streamadapter.NewMilestoneRecorder(store, nil)
	require.NoError(t, err)

	bus := memstream.New()
	backend := newTestBackendWithFacts(t, bus, fakeScriptStream, facts)

	spec := agentos.RunSpec{
		RunID:       Run1,
		ThreadID:    testThreadID,
		AccountID:   testAccountID,
		Backend:     backend.config.Ref(),
		UserMessage: testPromptText,
	}

	_, err = backend.Start(t.Context(), &spec)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		status, statusErr := backend.Status(t.Context(), Run1)

		return statusErr == nil && status.LifecycleState == stateFailed
	}, 10*time.Second, 20*time.Millisecond, "the run must end failed")

	require.Greater(t, store.calls, 1, "the failing write must have been attempted")
}

// TestBackendFailsRunWhenNotificationDropped locks the transport's side of the
// fail-hard rule: a dropped notification is a hole in the run's facts, so the
// run fails instead of finishing as if nothing were missing.
func TestBackendFailsRunWhenNotificationDropped(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t, nil, memstream.New(), fakeScriptStream)

	spec := agentos.RunSpec{
		RunID:       Run1,
		ThreadID:    testThreadID,
		AccountID:   testAccountID,
		Backend:     backend.config.Ref(),
		UserMessage: testPromptText,
	}

	_, err := backend.Start(t.Context(), &spec)
	require.NoError(t, err)

	state, ok := backend.run(Run1)
	require.True(t, ok)

	require.False(t, backend.failIfNotificationDropped(t.Context(), state), "a healthy run is not failed")

	backend.client.markDropped(state.sessionID)
	require.True(t, backend.failIfNotificationDropped(t.Context(), state), "a dropped notification fails the run")

	status, statusErr := backend.Status(t.Context(), Run1)
	require.NoError(t, statusErr)
	require.Equal(t, stateFailed, status.LifecycleState)
}

// TestSDKClientTracksDroppedNotifications locks the sticky flag the run failure
// is derived from, and that releasing a session forgets it.
func TestSDKClientTracksDroppedNotifications(t *testing.T) {
	t.Parallel()

	client := &sdkClient{sessions: map[string]chan sdkNotification{}}

	require.False(t, client.droppedNotification("s-1"))

	client.markDropped("s-1")
	require.True(t, client.droppedNotification("s-1"))
	require.False(t, client.droppedNotification("s-2"), "one session's loss is not another's")

	client.markDropped("s-2")
	client.unsubscribe("s-1")
	require.False(t, client.droppedNotification("s-1"), "a released session forgets its drops")
	require.True(t, client.droppedNotification("s-2"))
}
