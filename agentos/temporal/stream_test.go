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
