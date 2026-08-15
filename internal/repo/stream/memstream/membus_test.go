package memstream

import (
	"context"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/streamconformance"
)

func TestBusConformance(t *testing.T) {
	t.Parallel()

	handle := stream.NewHandle("agentos:run:acme:conformance-run")

	streamconformance.RunStreamConformance(t, &streamconformance.ConformanceCase{
		Name:   "memstream",
		Handle: handle,
		New: func() (stream.Publisher, stream.Subscriber) {
			bus := New()

			return bus, bus
		},
	})
}

// TestBusChannelsAreIsolated locks the per-run channel model: publishing to
// one run never leaks into another.
func TestBusChannelsAreIsolated(t *testing.T) {
	t.Parallel()

	bus := New()
	ctx := context.Background()

	runA := stream.NewHandle("agentos:run:acme:run-a")
	runB := stream.NewHandle("agentos:run:acme:run-b")

	if err := bus.Publish(ctx, runA, stream.NewRunStarted("t", "run-a")); err != nil {
		t.Fatalf("publish run-a: %v", err)
	}

	subB, err := bus.Subscribe(ctx, runB, 0)
	if err != nil {
		t.Fatalf("subscribe run-b: %v", err)
	}
	defer subB.Close()

	select {
	case ev := <-subB.C:
		t.Fatalf("run-b received %s from run-a", ev.Event.Type)
	case <-time.After(100 * time.Millisecond):
	}

	// run-a's own subscription sees its event.
	subA, err := bus.Subscribe(ctx, runA, 0)
	if err != nil {
		t.Fatalf("subscribe run-a: %v", err)
	}
	defer subA.Close()

	select {
	case ev := <-subA.C:
		if ev.Event.Type != stream.EventRunStarted {
			t.Fatalf("run-a event = %q, want RUN_STARTED", ev.Event.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("run-a did not receive its event")
	}
}

// TestBusHistoryIsBounded locks the transient-window semantics: old events
// drop out of the retained history, so replay-after-cursor cannot resurrect
// them — the durable projection (Postgres) owns authoritative history.
func TestBusHistoryIsBounded(t *testing.T) {
	t.Parallel()

	bus := New(WithHistory(3))
	ctx := context.Background()
	handle := stream.NewHandle("agentos:run:acme:bounded")

	for i := range 5 {
		if err := bus.Publish(ctx, handle, stream.NewTextMessageContent("t", "r", "m", "x")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	sub, err := bus.Subscribe(ctx, handle, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Only the last 3 survive the window; sequences must still be the
	// original ones (3, 4, 5), not renumbered.
	got := drainN(t, sub, 3)
	if got[0].Sequence != 3 || got[2].Sequence != 5 {
		t.Fatalf("retained sequences = %d..%d, want 3..5", got[0].Sequence, got[2].Sequence)
	}
}

// TestBusSlowConsumerDrops mirrors the Redis fan-out policy: a non-draining
// subscriber never blocks the publisher.
func TestBusSlowConsumerDrops(t *testing.T) {
	t.Parallel()

	bus := New(WithBuffer(1))
	ctx := context.Background()
	handle := stream.NewHandle("agentos:run:acme:slow")

	sub, err := bus.Subscribe(ctx, handle, stream.LiveOnly)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Flood the bus without draining; publishes must all succeed.
	for i := range 100 {
		if err := bus.Publish(ctx, handle, stream.NewTextMessageContent("t", "r", "m", "x")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
}

// TestBusLiveOnlyReplaysNothing locks the LiveOnly cursor semantics.
func TestBusLiveOnlyReplaysNothing(t *testing.T) {
	t.Parallel()

	bus := New()
	ctx := context.Background()
	handle := stream.NewHandle("agentos:run:acme:liveonly")

	// History exists, but a LiveOnly subscription must not replay it.
	publishN(t, bus, handle, 3)

	sub, err := bus.Subscribe(ctx, handle, stream.LiveOnly)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	select {
	case ev := <-sub.C:
		t.Fatalf("LiveOnly replayed %s sequence %d", ev.Event.Type, ev.Sequence)
	case <-time.After(100 * time.Millisecond):
	}

	// New publishes do arrive live, starting at the next sequence.
	publishN(t, bus, handle, 1)

	select {
	case ev := <-sub.C:
		if ev.Sequence != 4 {
			t.Fatalf("live event sequence = %d, want 4", ev.Sequence)
		}
	case <-time.After(time.Second):
		t.Fatal("live event not delivered")
	}
}

func publishN(t *testing.T, bus *Bus, handle *stream.Handle, n int) {
	t.Helper()

	for i := range n {
		if err := bus.Publish(context.Background(), handle, stream.NewTextMessageContent("t", "r", "m", "x")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
}

func drainN(t *testing.T, sub *stream.Subscription, n int) []stream.StoredEvent {
	t.Helper()

	got := make([]stream.StoredEvent, 0, n)

	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	for len(got) < n {
		select {
		case ev := <-sub.C:
			got = append(got, ev)
		case <-timer.C:
			t.Fatalf("drainN timed out: got %d/%d", len(got), n)
		}
	}

	return got
}
