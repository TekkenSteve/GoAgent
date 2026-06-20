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
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
		ThreadID:  "thread-1",
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

func TestPlanEventFromStateEventRequiresReducerTimestamp(t *testing.T) {
	_, _, err := PlanEventFromStateEvent(
		scopedEventTestPlanSpec("plan-1", ""),
		agentos.RunPlanStatus{
			PlanID:         "plan-1",
			LifecycleState: agentos.PlanLifecycleRunning,
			UpdatedAt:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
		},
		StateEvent{Kind: EventPlanStarted},
	)
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("PlanEventFromStateEvent error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestMemoryPlanStoreAppendPlanEventIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1", IdempotencyKey: "plan-start-1"}
	status := agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning}
	createMemoryPlanStateForEventTest(t, ctx, store, spec, status)

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
	if first.AccountID != spec.AccountID || first.ProjectID != spec.ProjectID {
		t.Fatalf("event scope = %s/%s, want %s/%s", first.AccountID, first.ProjectID, spec.AccountID, spec.ProjectID)
	}
	if _, err := agentos.MarshalPlanEvent(first); err != nil {
		t.Fatalf("stored event is not valid public PlanEvent: %v", err)
	}
	events, err := store.ListPlanEvents(ctx, agentos.PlanStreamScope{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}, 0)
	if err != nil {
		t.Fatalf("ListPlanEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
}

func TestMemoryPlanStoreAppendPlanEventScopesIdempotencyKeyByPlan(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	for _, planID := range []string{"plan-1", "plan-2"} {
		createMemoryPlanStateForEventTest(t, ctx, store, agentos.RunPlanSpec{
			PlanID:         planID,
			AccountID:      "acct-" + planID,
			ProjectID:      "proj-" + planID,
			IdempotencyKey: planID + "-start",
		}, agentos.RunPlanStatus{PlanID: planID, LifecycleState: agentos.PlanLifecycleRunning})
	}

	first, err := store.AppendPlanEvent(ctx, agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: "plan-1",
	}, "shared-key")
	if err != nil {
		t.Fatalf("AppendPlanEvent first plan: %v", err)
	}
	second, err := store.AppendPlanEvent(ctx, agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: "plan-2",
	}, "shared-key")
	if err != nil {
		t.Fatalf("AppendPlanEvent second plan: %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 1 || first.EventID != "plan-1:1" || second.EventID != "plan-2:1" {
		t.Fatalf("events = %#v %#v, want independent plan-scoped event identities", first, second)
	}
}

func TestMemoryPlanStoreAppendPlanEventAssignsStoreOwnedIdentity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	createMemoryPlanStateForEventTest(t, ctx, store,
		scopedEventTestPlanSpec("plan-1", "plan-start-1"),
		agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning},
	)
	event := agentos.PlanEvent{
		Event: agentos.Event{
			EventID:   "caller-event",
			EventType: agentos.EventPlanStarted,
			Sequence:  99,
		},
		PlanID: "plan-1",
	}

	first, err := store.AppendPlanEvent(ctx, event, "event-key")
	if err != nil {
		t.Fatalf("AppendPlanEvent first: %v", err)
	}
	replay, err := store.AppendPlanEvent(ctx, event, "event-key")
	if err != nil {
		t.Fatalf("AppendPlanEvent replay: %v", err)
	}
	if first.EventID != "plan-1:1" || first.Sequence != 1 {
		t.Fatalf("first event identity = %s/%d, want plan-1:1/1", first.EventID, first.Sequence)
	}
	if replay.EventID != first.EventID || replay.Sequence != first.Sequence {
		t.Fatalf("replay = %#v, want %#v", replay, first)
	}
}

func TestMemoryPlanStoreAppendPlanEventRequiresIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	createMemoryPlanStateForEventTest(t, ctx, store,
		scopedEventTestPlanSpec("plan-1", "plan-start-1"),
		agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning},
	)

	_, err := store.AppendPlanEvent(ctx, agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: "plan-1",
	}, "")
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("AppendPlanEvent empty key error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestMemoryPlanStoreAppendPlanEventRejectsMissingPlan(t *testing.T) {
	store := NewMemoryPlanStore()

	_, err := store.AppendPlanEvent(context.Background(), agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: "missing-plan",
	}, "event-1")
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("AppendPlanEvent missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStoreAppendPlanEventRejectsDifferentReplay(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := scopedEventTestPlanSpec("plan-1", "plan-start-1")
	status := agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning}
	createMemoryPlanStateForEventTest(t, ctx, store, spec, status)

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
	createMemoryPlanStateForEventTest(t, ctx, store, spec, status)
	if _, err := store.AppendPlanEvent(ctx, agentos.PlanEvent{Event: agentos.Event{EventType: agentos.EventPlanStarted}, PlanID: spec.PlanID}, "event-1"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}

	for _, scope := range []agentos.PlanStreamScope{
		{PlanID: spec.PlanID},
		{PlanID: spec.PlanID, AccountID: spec.AccountID},
		{PlanID: spec.PlanID, ProjectID: spec.ProjectID},
	} {
		if _, err := store.ListPlanEvents(ctx, scope, 0); !errors.Is(err, agentos.ErrInvalidPlanScope) {
			t.Fatalf("ListPlanEvents scope %#v error = %v, want ErrInvalidPlanScope", scope, err)
		}
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
		scopedEventTestPlanSpec("plan-1", ""),
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
				scopedEventTestPlanSpec("plan-1", ""),
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
		scopedEventTestPlanSpec("plan-1", ""),
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
	spec := scopedEventTestPlanSpec("plan-1", "")
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

func TestPlanTimeoutControlIdempotencyKeyRequiresStartedAt(t *testing.T) {
	startedAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	first, err := PlanTimeoutControlIdempotencyKey("plan-1", startedAt, 60)
	if err != nil {
		t.Fatalf("PlanTimeoutControlIdempotencyKey: %v", err)
	}
	second, err := PlanTimeoutControlIdempotencyKey("plan-1", startedAt, 60)
	if err != nil {
		t.Fatalf("PlanTimeoutControlIdempotencyKey replay: %v", err)
	}
	if first == "" || second != first {
		t.Fatalf("timeout control keys = %q/%q, want stable non-empty key", first, second)
	}
	if changed, err := PlanTimeoutControlIdempotencyKey("plan-1", startedAt.Add(time.Second), 60); err != nil || changed == first {
		t.Fatalf("changed started_at key = %q err=%v, want distinct key", changed, err)
	}
	if _, err := PlanTimeoutControlIdempotencyKey("plan-1", time.Time{}, 60); err == nil {
		t.Fatal("PlanTimeoutControlIdempotencyKey accepted empty start time")
	}
	if _, err := PlanTimeoutControlIdempotencyKey("plan-1", startedAt, 0); err == nil {
		t.Fatal("PlanTimeoutControlIdempotencyKey accepted empty timeout")
	}
}

func TestPlanSignalCancelControlIdempotencyKeyOnlyAcceptsReject(t *testing.T) {
	reject := agentos.Signal{
		Type:           agentos.SignalPlanReject,
		IdempotencyKey: "reject-1",
		ActorID:        "operator-1",
	}
	first, err := PlanSignalCancelControlIdempotencyKey("plan-1", reject)
	if err != nil {
		t.Fatalf("PlanSignalCancelControlIdempotencyKey: %v", err)
	}
	second, err := PlanSignalCancelControlIdempotencyKey("plan-1", reject)
	if err != nil {
		t.Fatalf("PlanSignalCancelControlIdempotencyKey replay: %v", err)
	}
	if first == "" || first != second {
		t.Fatalf("signal cancel keys = %q/%q, want stable non-empty key", first, second)
	}
	if _, err := PlanSignalCancelControlIdempotencyKey("plan-1", agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}); !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("approve signal key error = %v, want ErrInvalidSignal", err)
	}
}

func createMemoryPlanStateForEventTest(t *testing.T, ctx context.Context, store *MemoryPlanStore, spec agentos.RunPlanSpec, status agentos.RunPlanStatus) {
	t.Helper()

	spec = ensureEventTestPlanScope(spec)
	if _, _, err := store.CreatePlan(ctx, spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
}

func scopedEventTestPlanSpec(planID string, idempotencyKey string) agentos.RunPlanSpec {
	return ensureEventTestPlanScope(agentos.RunPlanSpec{
		PlanID:         planID,
		IdempotencyKey: idempotencyKey,
	})
}

func ensureEventTestPlanScope(spec agentos.RunPlanSpec) agentos.RunPlanSpec {
	if spec.AccountID == "" {
		spec.AccountID = "acct-" + spec.PlanID
	}
	if spec.ProjectID == "" {
		spec.ProjectID = "proj-" + spec.PlanID
	}

	return spec
}
