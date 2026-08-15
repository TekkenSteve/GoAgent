// Package streamconformance provides conformance helpers for data-plane bus
// implementations. Any bus that passes RunStreamConformance satisfies the
// agentos/stream contract — publish, ordered subscribe, replay-after-cursor,
// and projection reduction — and is a drop-in for the production Centrifugo
// transport.
package streamconformance

import (
	"context"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
)

const (
	// deliveryTimeout bounds how long a conformance drain waits for events.
	deliveryTimeout = 2 * time.Second
	// closedTimeout bounds how long we wait for a subscription to close.
	closedTimeout = 500 * time.Millisecond
)

// ConformanceCase describes how to build the bus under test. New is
// called once per sub-test so every scenario starts from a clean channel — a
// bus that carries history from one scenario into the next would mask
// delivery bugs.
type ConformanceCase struct {
	Name string
	// Handle is the channel to run scenarios against. May be shared across
	// sub-tests: each New() builds a fresh bus with empty history.
	Handle *stream.Handle
	// New returns a fresh Publisher+Subscriber pair per sub-test.
	New func() (stream.Publisher, stream.Subscriber)
}

// RunStreamConformance verifies the shared data-plane contract.
func RunStreamConformance(t *testing.T, tc *ConformanceCase) {
	t.Helper()

	if tc.New == nil {
		t.Fatal("New is required")
	}

	if tc.Handle == nil {
		tc.Handle = stream.NewHandle("agentos:run:acme:conformance-run")
	}

	if err := tc.Handle.Validate(); err != nil {
		t.Fatalf("invalid stream handle: %v", err)
	}

	t.Run(tc.Name+"/publish-then-replay", func(t *testing.T) { publishThenReplay(t, tc) })
	t.Run(tc.Name+"/subscribe-then-live", func(t *testing.T) { subscribeThenLive(t, tc) })
	t.Run(tc.Name+"/replay-after-cursor", func(t *testing.T) { replayAfterCursor(t, tc) })
	t.Run(tc.Name+"/replay-plus-live-order", func(t *testing.T) { replayPlusLiveOrder(t, tc) })
	t.Run(tc.Name+"/projection-reduces-to-milestones", func(t *testing.T) { projectionReducesToMilestones(t, tc) })
	t.Run(tc.Name+"/validation", func(t *testing.T) { validation(t, tc) })
	t.Run(tc.Name+"/close-stops-delivery", func(t *testing.T) { closeStopsDelivery(t, tc) })
}

// timeline is the canonical mixed event script the conformance suite publishes
// and asserts against: byte deltas interleaved with milestones.
func timeline() []*stream.Event {
	return []*stream.Event{
		stream.NewRunStarted("thread-1", "run-1"),
		stream.NewTextMessageContent("thread-1", "run-1", "m-1", "你好"),
		stream.NewReasoningMessageContent("thread-1", "run-1", "m-1", "思考中…"),
		stream.NewTextMessageEnd("thread-1", "run-1", "m-1"),
		stream.NewToolCallStart("thread-1", "run-1", "c-1", "read_file"),
		stream.NewToolCallArgs("thread-1", "run-1", "c-1", `{"path":`),
		stream.NewToolCallResult("thread-1", "run-1", "c-1", map[string]any{"ok": true}),
		stream.NewTextMessageContent("thread-1", "run-1", "m-2", "世界"),
		stream.NewRunFinished("thread-1", "run-1"),
	}
}

// expectedMilestones is the projection of timeline(): only milestone types,
// in order, mapped to the control plane's durable event types.
func expectedMilestones() []agentoscore.EventType {
	return []agentoscore.EventType{
		agentoscore.EventRunStarted,
		agentoscore.EventAgentMessageCompleted,
		agentoscore.EventToolCallStarted,
		agentoscore.EventToolCallCompleted,
		agentoscore.EventRunCompleted,
	}
}

func publishAll(t *testing.T, publisher stream.Publisher, handle *stream.Handle, events []*stream.Event) {
	t.Helper()

	ctx := context.Background()
	for _, ev := range events {
		if err := publisher.Publish(ctx, handle, ev); err != nil {
			t.Fatalf("Publish(%s): %v", ev.Type, err)
		}
	}
}

// drain reads exactly n stored events or fails after deliveryTimeout.
func drain(t *testing.T, sub *stream.Subscription, n int) []stream.StoredEvent {
	t.Helper()

	received := make([]stream.StoredEvent, 0, n)

	timer := time.NewTimer(deliveryTimeout)
	defer timer.Stop()

	for len(received) < n {
		select {
		case ev, ok := <-sub.C:
			if !ok {
				t.Fatalf("subscription closed early: got %d/%d events", len(received), n)
			}

			received = append(received, ev)
		case <-timer.C:
			t.Fatalf("timed out: got %d/%d events", len(received), n)
		}
	}

	return received
}

func assertSequences(t *testing.T, events []stream.StoredEvent, start int64) {
	t.Helper()

	for i := range events {
		want := start + int64(i)
		if events[i].Sequence != want {
			t.Fatalf("event %d sequence = %d, want %d", i, events[i].Sequence, want)
		}
	}
}

func assertTypes(t *testing.T, got []stream.StoredEvent, want []*stream.Event) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d", len(got), len(want))
	}

	for i := range got {
		if got[i].Event.Type != want[i].Type {
			t.Fatalf("event %d type = %q, want %q", i, got[i].Event.Type, want[i].Type)
		}

		if err := got[i].Event.Validate(); err != nil {
			t.Fatalf("event %d invalid: %v", i, err)
		}
	}
}

func subscribe(t *testing.T, subscriber stream.Subscriber, handle *stream.Handle, after int64) *stream.Subscription {
	t.Helper()

	sub, err := subscriber.Subscribe(context.Background(), handle, after)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	t.Cleanup(sub.Close)

	return sub
}

func publishThenReplay(t *testing.T, tc *ConformanceCase) {
	t.Helper()

	publisher, subscriber := tc.New()

	publishAll(t, publisher, tc.Handle, timeline())
	sub := subscribe(t, subscriber, tc.Handle, 0)

	got := drain(t, sub, len(timeline()))
	assertSequences(t, got, 1)
	assertTypes(t, got, timeline())
}

func subscribeThenLive(t *testing.T, tc *ConformanceCase) {
	t.Helper()

	publisher, subscriber := tc.New()

	sub := subscribe(t, subscriber, tc.Handle, stream.LiveOnly)

	events := timeline()
	publishAll(t, publisher, tc.Handle, events)

	got := drain(t, sub, len(events))
	assertSequences(t, got, 1)
	assertTypes(t, got, events)
}

func replayAfterCursor(t *testing.T, tc *ConformanceCase) {
	t.Helper()

	publisher, subscriber := tc.New()

	prefix := timeline()
	publishAll(t, publisher, tc.Handle, prefix)

	tail := timeline()
	publishAll(t, publisher, tc.Handle, tail)

	sub := subscribe(t, subscriber, tc.Handle, int64(len(prefix)))

	got := drain(t, sub, len(tail))
	assertSequences(t, got, int64(len(prefix)+1))
	assertTypes(t, got, tail)
}

func replayPlusLiveOrder(t *testing.T, tc *ConformanceCase) {
	t.Helper()

	publisher, subscriber := tc.New()

	prefix := timeline()
	publishAll(t, publisher, tc.Handle, prefix)

	// Subscribe after the second-to-last prefix event (seq = len-1): exactly
	// the last prefix event replays, everything after it arrives live — the
	// ordering across the replay→live bridge is what this test locks.
	sub := subscribe(t, subscriber, tc.Handle, int64(len(prefix)-1))

	tail := timeline()
	publishAll(t, publisher, tc.Handle, tail)

	total := 1 + len(tail)
	got := drain(t, sub, total)

	// First delivered is the last prefix event, sequence = len(prefix).
	assertSequences(t, got, int64(len(prefix)))
	assertTypes(t, got, append(prefix[len(prefix)-1:], tail...))
}

func projectionReducesToMilestones(t *testing.T, tc *ConformanceCase) {
	t.Helper()

	publisher, subscriber := tc.New()

	events := timeline()
	publishAll(t, publisher, tc.Handle, events)

	projector := stream.NewProjector(subscriber)

	sub, err := projector.Consume(context.Background(), tc.Handle, 0)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	t.Cleanup(sub.Close)

	got := drain(t, sub, len(events))

	var projected []*agentoscore.Event

	for i := range got {
		ev, ok := stream.ProjectToCore(tc.Handle, &got[i])
		if ok {
			projected = append(projected, ev)
		}
	}

	want := expectedMilestones()
	if len(projected) != len(want) {
		t.Fatalf("projection = %d events, want %d milestones", len(projected), len(want))
	}

	for i, ev := range projected {
		if ev.EventType != want[i] {
			t.Fatalf("projected[%d] type = %q, want %q", i, ev.EventType, want[i])
		}

		if ev.Source != tc.Handle.Channel {
			t.Fatalf("projected[%d] source = %q, want %q", i, ev.Source, tc.Handle.Channel)
		}
	}
}

func validation(t *testing.T, tc *ConformanceCase) {
	t.Helper()

	publisher, _ := tc.New()
	ctx := context.Background()

	if err := publisher.Publish(ctx, tc.Handle, stream.NewEvent("")); err == nil {
		t.Fatal("publish with empty event type: want error")
	}

	if err := publisher.Publish(ctx, tc.Handle, stream.NewEvent(stream.EventCustom)); err == nil {
		t.Fatal("publish CUSTOM without name: want error")
	}

	if err := publisher.Publish(ctx, nil, stream.NewRunStarted("t", "r")); err == nil {
		t.Fatal("publish with nil handle: want error")
	}
}

func closeStopsDelivery(t *testing.T, tc *ConformanceCase) {
	t.Helper()

	publisher, subscriber := tc.New()

	sub, err := subscriber.Subscribe(context.Background(), tc.Handle, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	publishAll(t, publisher, tc.Handle, timeline()[:1])

	got := drain(t, sub, 1)
	if got[0].Event.Type != stream.EventRunStarted {
		t.Fatalf("first event type = %q", got[0].Event.Type)
	}

	publishAll(t, publisher, tc.Handle, timeline()[1:2])
	drain(t, sub, 1) // pump now in live mode; e2 delivered

	sub.Close()

	// After Close the subscriber is out of the fan-out: a further publish must
	// not be delivered, and the channel must close on its own.
	publishAll(t, publisher, tc.Handle, timeline()[2:3])

	select {
	case _, ok := <-sub.C:
		if ok {
			t.Fatal("received event after Close")
		}
	case <-time.After(closedTimeout):
		t.Fatal("subscription did not close after Close")
	}
}
