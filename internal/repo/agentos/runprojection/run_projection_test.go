package runprojection

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
	"github.com/stretchr/testify/require"
)

// errProjectionStoreFailure stands in for a durable store that is down
// (err113: a static sentinel, not a per-test dynamic error).
var errProjectionStoreFailure = errors.New("projection store failure")

// fakeRunEventStore is a mutex-guarded in-memory RunEventStore with error
// injection: cursorFailures fails the first N cursor reads, appendFailures the
// first N appends. The projector's drain runs on another goroutine, so every
// access is under the lock.
type fakeRunEventStore struct {
	mu             sync.Mutex
	lastSequence   int64
	events         []agentoscore.Event
	cursorFailures int
	appendFailures int
}

func (s *fakeRunEventStore) LastRunEventSequence(context.Context, string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cursorFailures > 0 {
		s.cursorFailures--

		return 0, errProjectionStoreFailure
	}

	return s.lastSequence, nil
}

func (s *fakeRunEventStore) AppendRunEvent(_ context.Context, ev *agentoscore.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.appendFailures > 0 {
		s.appendFailures--

		return errProjectionStoreFailure
	}

	if ev.Sequence > s.lastSequence {
		s.lastSequence = ev.Sequence
	}

	s.events = append(s.events, *ev)

	return nil
}

func (s *fakeRunEventStore) eventTypes() []agentoscore.EventType {
	s.mu.Lock()
	defer s.mu.Unlock()

	types := make([]agentoscore.EventType, len(s.events))
	for i := range s.events {
		types[i] = s.events[i].EventType
	}

	return types
}

func (s *fakeRunEventStore) sequences() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	seqs := make([]int64, len(s.events))
	for i := range s.events {
		seqs[i] = s.events[i].Sequence
	}

	return seqs
}

func (s *fakeRunEventStore) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.events)
}

func newProjector(t *testing.T, bus *memstream.Bus, store RunEventStore) *RunEventProjector {
	t.Helper()

	p, err := New(t.Context(), Config{Subscriber: bus, Store: store})
	require.NoError(t, err)

	t.Cleanup(p.Close)

	return p
}

func runHandle(runID string) *stream.Handle {
	return stream.NewHandle("agentos:run:acme:" + runID)
}

// TestRunEventProjectorProjectsMilestones drives one run's full timeline
// through the projector and asserts the durable projection carries exactly the
// milestones — transient byte deltas excluded — with the bus offset stamped as
// Sequence and the run channel as Source. The terminal milestone then
// self-closes the subscription.
func TestRunEventProjectorProjectsMilestones(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	handle := runHandle("run-1")
	store := &fakeRunEventStore{}
	p := newProjector(t, bus, store)

	require.NoError(t, p.EnsureSubscribed(t.Context(), "run-1", handle))

	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunStarted("sess-1", "run-1")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewTextMessageContent("sess-1", "run-1", "m-1", "hello")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewToolCallStart("sess-1", "run-1", "call-1", "bash")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewToolCallResult("sess-1", "run-1", "call-1", "done")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunFinished("sess-1", "run-1")))

	require.Eventually(t, func() bool { return store.len() == 4 }, 2*time.Second, 10*time.Millisecond)

	require.Equal(t, []agentoscore.EventType{
		agentoscore.EventRunStarted,
		agentoscore.EventToolCallStarted,
		agentoscore.EventToolCallCompleted,
		agentoscore.EventRunCompleted,
	}, store.eventTypes())

	// Sequence 2 (TEXT_MESSAGE_CONTENT) is transient and never projected.
	require.Equal(t, []int64{1, 3, 4, 5}, store.sequences())

	store.mu.Lock()
	first := store.events[0]
	store.mu.Unlock()

	require.Equal(t, "run-1:1", first.EventID, "event id mirrors the plan event shape")
	require.Equal(t, "sess-1", first.ThreadID, "thread scope projected")
	require.Equal(t, "agentos:run:acme:run-1", first.Source, "source channel projected for audit linkage")

	// The terminal milestone self-closes: no subscription lingers after the run.
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()

		return len(p.subs) == 0
	}, 2*time.Second, 10*time.Millisecond)
}

// TestRunEventProjectorResumesFromCursor detaches a projector mid-run, lets the
// run continue, then re-attaches a fresh projector: it resumes from the
// persisted cursor and replays only what it has not yet projected.
func TestRunEventProjectorResumesFromCursor(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	handle := runHandle("run-2")
	store := &fakeRunEventStore{}

	p, err := New(t.Context(), Config{Subscriber: bus, Store: store})
	require.NoError(t, err)
	require.NoError(t, p.EnsureSubscribed(t.Context(), "run-2", handle))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunStarted("sess-1", "run-2")))
	require.Eventually(t, func() bool { return store.len() == 1 }, 2*time.Second, 10*time.Millisecond)
	p.Close()

	// The run continues on the bus while the projection is detached.
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewToolCallStart("sess-1", "run-2", "call-1", "bash")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunFinished("sess-1", "run-2")))

	// A fresh projector re-attaches from the persisted cursor (sequence 1).
	p2 := newProjector(t, bus, store)
	require.NoError(t, p2.EnsureSubscribed(t.Context(), "run-2", handle))

	require.Eventually(t, func() bool { return store.len() == 3 }, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, []int64{1, 2, 3}, store.sequences(), "seq1 is not duplicated on re-attach")
}

// TestRunEventProjectorEnsureSubscribedIsIdempotent locks the register-by-runID
// dedup: a second attach is a no-op, so each milestone lands exactly once.
func TestRunEventProjectorEnsureSubscribedIsIdempotent(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	handle := runHandle("run-3")
	store := &fakeRunEventStore{}
	p := newProjector(t, bus, store)

	require.NoError(t, p.EnsureSubscribed(t.Context(), "run-3", handle))
	require.NoError(t, p.EnsureSubscribed(t.Context(), "run-3", handle))

	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunStarted("sess-1", "run-3")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunFinished("sess-1", "run-3")))

	require.Eventually(t, func() bool { return store.len() == 2 }, 2*time.Second, 10*time.Millisecond)
}

// TestRunEventProjectorAppendFailureIsFailOpen breaks the durable store for the
// first milestone's initial append and its retry: the drain logs and continues
// rather than aborting the run's timeline, and the terminal milestone still
// self-closes the subscription.
func TestRunEventProjectorAppendFailureIsFailOpen(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	handle := runHandle("run-4")
	store := &fakeRunEventStore{appendFailures: 2}
	p := newProjector(t, bus, store)

	require.NoError(t, p.EnsureSubscribed(t.Context(), "run-4", handle))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunStarted("sess-1", "run-4")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewToolCallStart("sess-1", "run-4", "call-1", "bash")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunFinished("sess-1", "run-4")))

	require.Eventually(t, func() bool { return store.len() == 2 }, 2*time.Second, 10*time.Millisecond)

	store.mu.Lock()
	require.Equal(t, agentoscore.EventToolCallStarted, store.events[0].EventType, "failed run.started skipped")
	store.mu.Unlock()

	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()

		return len(p.subs) == 0
	}, 2*time.Second, 10*time.Millisecond)
}

// TestRunEventProjectorSubscribeFailureRetriesOnNextEnsure fails the cursor
// read on the first attach and asserts the next publish path re-attaches: a
// one-off store hiccup never gates the run.
func TestRunEventProjectorSubscribeFailureRetriesOnNextEnsure(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	handle := runHandle("run-5")
	store := &fakeRunEventStore{cursorFailures: 1}
	p := newProjector(t, bus, store)

	require.Error(t, p.EnsureSubscribed(t.Context(), "run-5", handle))

	require.NoError(t, p.EnsureSubscribed(t.Context(), "run-5", handle))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunStarted("sess-1", "run-5")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunFinished("sess-1", "run-5")))

	require.Eventually(t, func() bool { return store.len() == 2 }, 2*time.Second, 10*time.Millisecond)
}

// TestRunEventProjectorCloseTerminates locks the shutdown contract: Close with
// a live subscription returns (no hang), and a second Close is a no-op.
func TestRunEventProjectorCloseTerminates(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	store := &fakeRunEventStore{}

	p, err := New(t.Context(), Config{Subscriber: bus, Store: store})
	require.NoError(t, err)

	require.NoError(t, p.EnsureSubscribed(t.Context(), "run-6", runHandle("run-6")))

	done := make(chan struct{})

	go func() {
		p.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung with a live subscription")
	}
}

// TestRunEventProjectorNewRequiresSubscriberAndStore locks the constructor's
// sentinel validation.
func TestRunEventProjectorNewRequiresSubscriberAndStore(t *testing.T) {
	t.Parallel()

	_, err := New(t.Context(), Config{Subscriber: nil, Store: &fakeRunEventStore{}})
	require.ErrorIs(t, err, errRunProjectionSubscriberRequired)

	_, err = New(t.Context(), Config{Subscriber: memstream.New(), Store: nil})
	require.ErrorIs(t, err, errRunProjectionStoreRequired)
}

// TestRunEventProjectorCanceledMilestoneSelfCloses locks the cancel exit path:
// a run that publishes a canceled milestone closes its projection exactly like
// a completed or failed one, so a canceled run never lingers as a live drain.
func TestRunEventProjectorCanceledMilestoneSelfCloses(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	handle := runHandle("run-8")
	store := &fakeRunEventStore{}
	p := newProjector(t, bus, store)

	require.NoError(t, p.EnsureSubscribed(t.Context(), "run-8", handle))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunStarted("sess-1", "run-8")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunCanceled("sess-1", "run-8")))

	require.Eventually(t, func() bool { return store.len() == 2 }, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, []agentoscore.EventType{
		agentoscore.EventRunStarted,
		agentoscore.EventRunCanceled,
	}, store.eventTypes())

	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()

		return len(p.subs) == 0
	}, 2*time.Second, 10*time.Millisecond)
}

// TestRunEventProjectorReapsStaleProjection locks the stale reaper fallback for
// runs that exit without a terminal milestone: a projection idle past the
// timeout is closed (its drain unwinds) even though no terminal event ever
// arrives, and the next publish re-attaches from the persisted cursor.
func TestRunEventProjectorReapsStaleProjection(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	handle := runHandle("run-9")
	store := &fakeRunEventStore{}

	p, err := New(t.Context(), Config{
		Subscriber:            bus,
		Store:                 store,
		ReapInterval:          20 * time.Millisecond,
		ProjectionIdleTimeout: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(p.Close)

	require.NoError(t, p.EnsureSubscribed(t.Context(), "run-9", handle))

	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()

		return len(p.subs) == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Age the projection past the idle timeout so the next reap closes it.
	p.mu.Lock()
	p.subs["run-9"].lastActivity.Store(0)
	p.mu.Unlock()

	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()

		return len(p.subs) == 0
	}, 2*time.Second, 10*time.Millisecond)

	// Re-attach is fail-open: the next publish resumes from the persisted
	// cursor (nothing had been projected before the reap, so both milestones
	// land once).
	require.NoError(t, p.EnsureSubscribed(t.Context(), "run-9", handle))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunStarted("sess-1", "run-9")))
	require.NoError(t, bus.Publish(t.Context(), handle, stream.NewRunCanceled("sess-1", "run-9")))

	require.Eventually(t, func() bool { return store.len() == 2 }, 2*time.Second, 10*time.Millisecond)
}
