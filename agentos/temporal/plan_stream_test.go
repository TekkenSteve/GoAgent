package temporal

import (
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

const eventLive = "evt-live"

func TestNewPlanReplaySubscriptionPreservesPlanScopeInPayload(t *testing.T) {
	t.Parallel()

	sub := newPlanReplaySubscription([]agentos.PlanEvent{
		{
			Event: agentos.Event{
				EventID:   "evt-1",
				EventType: agentos.EventPlanNodeStarted,
				RunID:     "run-1",
				Sequence:  1,
				Payload:   map[string]any{"existing": true},
			},
			PlanID:    "plan-1",
			AccountID: "acct-1",
			ProjectID: "proj-1",
			NodeID:    "node-1",
		},
	})

	event, ok := <-sub.Events()
	if !ok {
		t.Fatal("subscription closed before replay event")
	}

	if event.Payload["plan_id"] != Plan1 ||
		event.Payload["account_id"] != "acct-1" ||
		event.Payload["project_id"] != "proj-1" ||
		event.Payload["node_id"] != "node-1" ||
		event.Payload["existing"] != true {
		t.Fatalf("payload = %#v", event.Payload)
	}

	if _, ok := <-sub.Events(); ok {
		t.Fatal("subscription should close after replay")
	}
}

func TestNewPlanReplayThenLiveSubscription(t *testing.T) {
	t.Parallel()

	liveEvents := make(chan agentos.PlanEvent, 1)
	live := &fakePlanEventSubscription{events: liveEvents}
	scope := agentos.PlanStreamScope{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}
	sub := newPlanReplayThenLiveSubscription(&scope, []agentos.PlanEvent{
		{
			Event: agentos.Event{
				EventID:   "evt-replay",
				EventType: agentos.EventPlanStarted,
				Sequence:  1,
			},
			PlanID:    "plan-1",
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
	}, live)

	replay := <-sub.Events()
	if replay.EventID != "evt-replay" || replay.Payload["plan_id"] != "plan-1" {
		t.Fatalf("replay event = %#v", replay)
	}

	liveEvents <- agentos.PlanEvent{
		Event: agentos.Event{
			EventID:   eventLive,
			EventType: agentos.EventPlanNodeStarted,
			Sequence:  2,
		},
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	}

	gotLive := <-sub.Events()
	if gotLive.EventID != eventLive {
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
	t.Parallel()

	liveEvents := make(chan agentos.PlanEvent, 3)
	live := &fakePlanEventSubscription{events: liveEvents}
	scope := agentos.PlanStreamScope{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}
	sub := newPlanReplayThenLiveSubscriptionAfter(&scope, []agentos.PlanEvent{
		{
			Event: agentos.Event{
				EventID:   "evt-catch-up",
				EventType: agentos.EventPlanNodeStarted,
				Sequence:  2,
			},
			PlanID:    "plan-1",
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
	}, live, 2)

	replay := <-sub.Events()
	if replay.EventID != "evt-catch-up" {
		t.Fatalf("replay event = %#v", replay)
	}

	liveEvents <- agentos.PlanEvent{
		Event: agentos.Event{
			EventID:   "evt-duplicate",
			EventType: agentos.EventPlanNodeStarted,
			Sequence:  2,
		},
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	}

	liveEvents <- agentos.PlanEvent{
		Event: agentos.Event{
			EventID:   "evt-other-plan",
			EventType: agentos.EventPlanNodeStarted,
			Sequence:  3,
		},
		PlanID:    "plan-other",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	}

	liveEvents <- agentos.PlanEvent{
		Event: agentos.Event{
			EventID:   eventLive,
			EventType: agentos.EventPlanNodeSucceeded,
			Sequence:  4,
		},
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	}

	gotLive := <-sub.Events()
	if gotLive.EventID != eventLive {
		t.Fatalf("live event = %#v", gotLive)
	}
}

func TestLastPlanEventSequence(t *testing.T) {
	t.Parallel()

	last := lastPlanEventSequence(3, []agentos.PlanEvent{
		{Event: agentos.Event{Sequence: 2}},
		{Event: agentos.Event{Sequence: 5}},
	})
	if last != 5 {
		t.Fatalf("last = %d, want 5", last)
	}
}

type fakePlanEventSubscription struct {
	events <-chan agentos.PlanEvent
	closed bool
}

func (s *fakePlanEventSubscription) Events() <-chan agentos.PlanEvent {
	return s.events
}

func (s *fakePlanEventSubscription) Close() error {
	s.closed = true

	return nil
}
