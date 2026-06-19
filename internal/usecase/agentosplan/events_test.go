package agentosplan

import (
	"context"
	"errors"
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
	if event.Payload[planEventPayloadPlanID] != "plan-1" || event.Payload[planEventPayloadNodeID] != "node-1" || event.Payload[planEventPayloadRunID] != "run-1" {
		t.Fatalf("payload = %#v", event.Payload)
	}
}

func TestMemoryPlanStoreAppendPlanEventIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{PlanID: "plan-1", IdempotencyKey: "plan-start-1"}
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

func TestMemoryPlanStoreAppendPlanEventRequiresIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	if err := store.SavePlanState(ctx, PlanStateSnapshot{
		Spec:   agentos.RunPlanSpec{PlanID: "plan-1", IdempotencyKey: "plan-start-1"},
		Status: agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning},
	}); err != nil {
		t.Fatalf("SavePlanState: %v", err)
	}

	_, err := store.AppendPlanEvent(ctx, agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: "plan-1",
	}, "")
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("AppendPlanEvent empty key error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestMemoryPlanStoreAppendPlanEventRejectsDifferentReplay(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{PlanID: "plan-1", IdempotencyKey: "plan-start-1"}
	status := agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning}
	if err := store.SavePlanState(ctx, PlanStateSnapshot{Spec: spec, Status: status}); err != nil {
		t.Fatalf("SavePlanState: %v", err)
	}

	event := agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted, Payload: map[string]any{"state": "started"}},
		PlanID: "plan-1",
	}
	if _, err := store.AppendPlanEvent(ctx, event, "key-1"); err != nil {
		t.Fatalf("AppendPlanEvent first: %v", err)
	}
	event.EventType = agentos.EventPlanFailed
	event.Payload = map[string]any{"state": "failed"}
	if _, err := store.AppendPlanEvent(ctx, event, "key-1"); !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("AppendPlanEvent changed replay error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestMemoryPlanStoreListPlanEventsEnforcesTenantScope(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1", IdempotencyKey: "plan-start-1"}
	status := agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning}
	if err := store.SavePlanState(ctx, PlanStateSnapshot{Spec: spec, Status: status}); err != nil {
		t.Fatalf("SavePlanState: %v", err)
	}
	if _, err := store.AppendPlanEvent(ctx, agentos.PlanEvent{Event: agentos.Event{EventType: agentos.EventPlanStarted}, PlanID: spec.PlanID}, "event-1"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}

	if _, err := store.ListPlanEvents(ctx, agentos.PlanStreamScope{PlanID: spec.PlanID, AccountID: "acct-other", ProjectID: spec.ProjectID}, 0); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("ListPlanEvents mismatch error = %v, want ErrPlanRouteNotFound", err)
	}
	events, err := store.ListPlanEvents(ctx, agentos.PlanStreamScope{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}, 0)
	if err != nil {
		t.Fatalf("ListPlanEvents scoped: %v", err)
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

func TestPlanEventFromApprovalSignals(t *testing.T) {
	at := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		kind EventKind
		want agentos.EventType
	}{
		{name: "approved", kind: EventPlanApproved, want: agentos.EventPlanApproved},
		{name: "rejected", kind: EventPlanRejected, want: agentos.EventPlanRejected},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, _, err := PlanEventFromStateEvent(
				agentos.RunPlanSpec{PlanID: "plan-1"},
				agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning, UpdatedAt: at},
				StateEvent{Kind: tt.kind, Reason: "operator decision", At: at},
			)
			if err != nil {
				t.Fatalf("PlanEventFromStateEvent: %v", err)
			}
			if event.EventType != tt.want {
				t.Fatalf("event type = %q, want %q", event.EventType, tt.want)
			}
		})
	}
}

func TestPlanEventFromBudgetReportedUsesPublicUsageEvent(t *testing.T) {
	at := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	event, _, err := PlanEventFromStateEvent(
		agentos.RunPlanSpec{PlanID: "plan-1"},
		agentos.RunPlanStatus{
			PlanID:         "plan-1",
			LifecycleState: agentos.PlanLifecycleRunning,
			BudgetUsage:    agentos.PlanBudgetUsage{SpentCents: 75},
			UpdatedAt:      at,
		},
		StateEvent{
			Kind:        EventBudgetReported,
			NodeID:      "node-1",
			RunID:       "run-1",
			BudgetDelta: agentos.PlanBudgetUsage{SpentCents: 25},
			At:          at,
		},
	)
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent: %v", err)
	}
	if event.EventType != agentos.EventUsageReported {
		t.Fatalf("event type = %q", event.EventType)
	}
	if event.Payload["budget_delta"] == nil || event.Payload["budget_usage"] == nil {
		t.Fatalf("payload missing budget usage = %#v", event.Payload)
	}
}

func TestPlanEventFromDebugTraceEvents(t *testing.T) {
	at := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	spec := agentos.RunPlanSpec{PlanID: "plan-1"}
	status := agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning, UpdatedAt: at}

	capabilityEvent, _, err := PlanEventFromStateEvent(spec, status, StateEvent{
		Kind:                   EventCapabilitySelected,
		NodeID:                 "node-1",
		Capability:             CapabilitySelectionTrace{Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}, Capability: "run", HasOutputSchema: true},
		PreviousLifecycleState: agentos.PlanNodePending,
		NextLifecycleState:     agentos.PlanNodePending,
		At:                     at,
	})
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent capability: %v", err)
	}
	if capabilityEvent.EventType != agentos.EventCapabilitySelected || capabilityEvent.Payload[planEventPayloadCapability] == nil {
		t.Fatalf("capability event = %#v", capabilityEvent)
	}
	if capabilityEvent.Payload[planEventPayloadTransition] == nil {
		t.Fatalf("capability event missing transition = %#v", capabilityEvent.Payload)
	}

	inputEvent, _, err := PlanEventFromStateEvent(spec, status, StateEvent{
		Kind:   EventNodeInputResolved,
		NodeID: "node-1",
		RunID:  "run-1",
		InputTrace: InputResolutionTrace{
			InputDigest:  "abc123",
			InputKeys:    []string{"topic"},
			MappingCount: 1,
			Mappings: []InputMappingTrace{
				{Target: "topic", SourcePath: "task.topic", Required: true},
			},
		},
		At: at,
	})
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent input: %v", err)
	}
	if inputEvent.EventType != agentos.EventNodeInputResolved || inputEvent.Payload[planEventPayloadInputResolution] == nil {
		t.Fatalf("input event = %#v", inputEvent)
	}

	conditionEvent, _, err := PlanEventFromStateEvent(spec, status, StateEvent{
		Kind: EventConditionsEvaluated,
		ConditionTraces: []ConditionEvaluationTrace{
			{Scope: "node", NodeID: "node-1", Expression: "inputs.enabled", Result: true},
		},
		At: at,
	})
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent conditions: %v", err)
	}
	if conditionEvent.EventType != agentos.EventConditionEvaluated || conditionEvent.Payload[planEventPayloadConditions] == nil {
		t.Fatalf("condition event = %#v", conditionEvent)
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
