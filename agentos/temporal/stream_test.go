package temporal

import (
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentfwstream "github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

func TestEventFromStored(t *testing.T) {
	ts := time.Date(2026, 6, 14, 13, 0, 0, 0, time.UTC)

	event := entity.TextDeltaEvent{
		BaseEvent: entity.BaseEvent{
			EventID:   "evt-1",
			RunID:     "run-1",
			SessionID: "thread-1",
			Source:    entity.SourceLLM,
			Timestamp: ts,
		},
		Content: "hello",
		Index:   1,
	}

	got := eventFromStored(agentfwstream.StoredEvent{
		Event:    event,
		Sequence: 42,
		StoredAt: ts,
	})

	if got.EventID != "evt-1" ||
		got.EventType != "llm.text.delta" ||
		got.RunID != "run-1" ||
		got.ThreadID != "thread-1" ||
		got.Source != "llm" ||
		got.Sequence != 42 ||
		!got.Timestamp.Equal(ts) {
		t.Fatalf("unexpected event mapping: %#v", got)
	}

	if got.Payload["content"] != "hello" {
		t.Fatalf("payload content = %#v", got.Payload["content"])
	}
}

func TestNewPlanReplaySubscriptionPreservesPlanScopeInPayload(t *testing.T) {
	sub := newPlanReplaySubscription([]agentos.PlanEvent{
		{
			Event: agentos.Event{
				EventID:   "evt-1",
				EventType: agentos.EventPlanNodeStarted,
				RunID:     "run-1",
				Sequence:  1,
				Payload:   map[string]any{"existing": true},
			},
			PlanID: "plan-1",
			NodeID: "node-1",
		},
	})

	event, ok := <-sub.Events()
	if !ok {
		t.Fatal("subscription closed before replay event")
	}
	if event.Payload["plan_id"] != "plan-1" || event.Payload["node_id"] != "node-1" || event.Payload["existing"] != true {
		t.Fatalf("payload = %#v", event.Payload)
	}
	if _, ok := <-sub.Events(); ok {
		t.Fatal("subscription should close after replay")
	}
}

func TestNewPlanReplayThenLiveSubscription(t *testing.T) {
	liveEvents := make(chan agentos.Event, 1)
	live := &fakeAgentOSSubscription{events: liveEvents}
	sub := newPlanReplayThenLiveSubscription([]agentos.PlanEvent{
		{
			Event: agentos.Event{
				EventID:   "evt-replay",
				EventType: agentos.EventPlanStarted,
				Sequence:  1,
			},
			PlanID: "plan-1",
		},
	}, live)

	replay := <-sub.Events()
	if replay.EventID != "evt-replay" || replay.Payload["plan_id"] != "plan-1" {
		t.Fatalf("replay event = %#v", replay)
	}

	liveEvents <- agentos.Event{EventID: "evt-live", EventType: agentos.EventPlanNodeStarted, Sequence: 2}
	gotLive := <-sub.Events()
	if gotLive.EventID != "evt-live" {
		t.Fatalf("live event = %#v", gotLive)
	}

	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !live.closed {
		t.Fatal("live subscription was not closed")
	}
}

func TestNewPlanReplayThenLiveSubscriptionAfterFiltersCoveredLiveEvents(t *testing.T) {
	liveEvents := make(chan agentos.Event, 2)
	live := &fakeAgentOSSubscription{events: liveEvents}
	sub := newPlanReplayThenLiveSubscriptionAfter([]agentos.PlanEvent{
		{
			Event: agentos.Event{
				EventID:   "evt-catch-up",
				EventType: agentos.EventPlanNodeStarted,
				Sequence:  2,
			},
			PlanID: "plan-1",
		},
	}, live, 2)

	replay := <-sub.Events()
	if replay.EventID != "evt-catch-up" {
		t.Fatalf("replay event = %#v", replay)
	}
	liveEvents <- agentos.Event{EventID: "evt-duplicate", EventType: agentos.EventPlanNodeStarted, Sequence: 2}
	liveEvents <- agentos.Event{EventID: "evt-live", EventType: agentos.EventPlanNodeSucceeded, Sequence: 3}

	gotLive := <-sub.Events()
	if gotLive.EventID != "evt-live" {
		t.Fatalf("live event = %#v", gotLive)
	}
}

func TestLastPlanEventSequence(t *testing.T) {
	last := lastPlanEventSequence(3, []agentos.PlanEvent{
		{Event: agentos.Event{Sequence: 2}},
		{Event: agentos.Event{Sequence: 5}},
	})
	if last != 5 {
		t.Fatalf("last = %d, want 5", last)
	}
}

type fakeAgentOSSubscription struct {
	events <-chan agentos.Event
	closed bool
}

func (s *fakeAgentOSSubscription) Events() <-chan agentos.Event {
	return s.events
}

func (s *fakeAgentOSSubscription) Close() error {
	s.closed = true

	return nil
}
