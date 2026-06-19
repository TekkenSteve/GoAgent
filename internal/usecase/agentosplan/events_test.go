package agentosplan

import (
	"context"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestPlanEventFromStateEventMapsPublicEvent(t *testing.T) {
	spec := agentos.RunPlanSpec{
		PlanID:   "plan-1",
		ThreadID: "thread-1",
	}
	status := agentos.RunPlanStatus{
		PlanID:         "plan-1",
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC),
	}

	event, key, err := PlanEventFromStateEvent(spec, status, StateEvent{
		Kind:   EventNodeStarted,
		NodeID: "node-1",
		RunID:  "run-1",
		At:     status.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent: %v", err)
	}
	if key == "" {
		t.Fatal("idempotency key is empty")
	}
	if event.EventType != agentos.EventPlanNodeStarted || event.PlanID != "plan-1" || event.NodeID != "node-1" || event.RunID != "run-1" {
		t.Fatalf("event = %#v", event)
	}
	if event.Payload["plan_id"] != "plan-1" || event.Payload["node_id"] != "node-1" || event.Payload["run_id"] != "run-1" {
		t.Fatalf("payload = %#v", event.Payload)
	}
}

func TestMemoryPlanStoreAppendPlanEventIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{PlanID: "plan-1"}
	status := agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning}
	if err := store.SavePlanState(ctx, PlanStateSnapshot{Spec: spec, Status: status}); err != nil {
		t.Fatalf("SavePlanState: %v", err)
	}

	event := agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: "plan-1",
	}
	first, err := store.AppendPlanEvent(ctx, event, "key-1")
	if err != nil {
		t.Fatalf("AppendPlanEvent first: %v", err)
	}
	second, err := store.AppendPlanEvent(ctx, event, "key-1")
	if err != nil {
		t.Fatalf("AppendPlanEvent second: %v", err)
	}
	if first.EventID != second.EventID || first.Sequence != second.Sequence {
		t.Fatalf("events are not idempotent: %#v %#v", first, second)
	}
	events, err := store.ListPlanEvents(ctx, agentos.PlanStreamScope{PlanID: "plan-1"}, 0)
	if err != nil {
		t.Fatalf("ListPlanEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
}

func TestPlanEventFromRetryScheduledIncludesAttempt(t *testing.T) {
	at := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	event, _, err := PlanEventFromStateEvent(
		agentos.RunPlanSpec{PlanID: "plan-1"},
		agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning, UpdatedAt: at},
		StateEvent{
			Kind:    EventNodeRetryScheduled,
			NodeID:  "node-1",
			RunID:   "run-1",
			Reason:  "failed",
			Attempt: 2,
			At:      at,
		},
	)
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent: %v", err)
	}
	if event.EventType != agentos.EventPlanNodeRetryScheduled {
		t.Fatalf("event type = %q", event.EventType)
	}
	if event.Payload["attempt"] != int32(2) {
		t.Fatalf("payload = %#v", event.Payload)
	}
}

func TestNodeStartIdempotencyKeyIncludesAttempt(t *testing.T) {
	first, err := NodeStartIdempotencyKey("plan-1", "node-1", 1)
	if err != nil {
		t.Fatalf("NodeStartIdempotencyKey first: %v", err)
	}
	second, err := NodeStartIdempotencyKey("plan-1", "node-1", 2)
	if err != nil {
		t.Fatalf("NodeStartIdempotencyKey second: %v", err)
	}
	if first == second {
		t.Fatalf("attempt-specific keys should differ: %q", first)
	}
	if _, err := NodeStartIdempotencyKey("plan-1", "node-1", 0); err == nil {
		t.Fatal("NodeStartIdempotencyKey accepted attempt 0")
	}
}

func TestNodeTimeoutControlIdempotencyKeyRequiresRunID(t *testing.T) {
	key, err := NodeTimeoutControlIdempotencyKey("plan-1", "node-1", "run-1")
	if err != nil {
		t.Fatalf("NodeTimeoutControlIdempotencyKey: %v", err)
	}
	if key == "" {
		t.Fatal("timeout control idempotency key is empty")
	}
	if _, err := NodeTimeoutControlIdempotencyKey("plan-1", "node-1", ""); err == nil {
		t.Fatal("NodeTimeoutControlIdempotencyKey accepted empty run id")
	}
}
