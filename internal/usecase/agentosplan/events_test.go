package agentosplan

import (
	"context"
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

const (
	Plan1   = "plan-1"
	Node1   = "node-1"
	Run1    = "run-1"
	Plan1_1 = "plan-1:1"
)

func planEventFromStateEventForTest(
	spec *agentos.RunPlanSpec,
	status *agentos.RunPlanStatus,
	event *StateEvent,
) (agentos.PlanEvent, string, error) {
	return PlanEventFromStateEvent(spec, status, event)
}

func TestPlanEventFromStateEventMapsPublicEvent(t *testing.T) {
	t.Parallel()

	spec := agentos.RunPlanSpec{
		PlanID:    Plan1,
		AccountID: "acct-1",
		ProjectID: "proj-1",
		ThreadID:  "thread-1",
	}
	status := agentos.RunPlanStatus{
		PlanID:         Plan1,
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC),
	}

	event, key, err := planEventFromStateEventForTest(&spec, &status, &StateEvent{
		Kind:   EventNodeStarted,
		NodeID: Node1,
		RunID:  Run1,
		At:     status.UpdatedAt,
	})
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent: %v", err)
	}

	if key == "" {
		t.Fatal("idempotency key is empty")
	}

	if event.EventType != agentoscore.EventPlanNodeStarted || event.PlanID != Plan1 || event.NodeID != Node1 || event.RunID != Run1 {
		t.Fatalf("event = %#v", event)
	}

	if event.Payload[planEventPayloadPlanID] != Plan1 || event.Payload[planEventPayloadNodeID] != Node1 || event.Payload[planEventPayloadRunID] != Run1 {
		t.Fatalf("payload = %#v", event.Payload)
	}
}

func TestPlanEventFromStateEventRequiresReducerTimestamp(t *testing.T) {
	t.Parallel()

	spec := scopedEventTestPlanSpec("")

	_, _, err := planEventFromStateEventForTest(
		&spec,
		&agentos.RunPlanStatus{
			PlanID:         Plan1,
			LifecycleState: agentos.PlanLifecycleRunning,
			UpdatedAt:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
		},
		&StateEvent{Kind: EventPlanStarted},
	)
	if !errors.Is(err, agentoscore.ErrInvalidPlanEvent) {
		t.Fatalf("PlanEventFromStateEvent error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestMemoryPlanStoreAppendPlanEventIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{PlanID: Plan1, AccountID: "acct-1", ProjectID: "proj-1", IdempotencyKey: "plan-start-1"}
	status := agentos.RunPlanStatus{PlanID: Plan1, LifecycleState: agentos.PlanLifecycleRunning}
	createMemoryPlanStateForEventTest(ctx, t, store, &spec, &status)

	event := agentos.PlanEvent{
		Event:  agentoscore.Event{EventType: agentoscore.EventPlanStarted},
		PlanID: Plan1,
	}

	first, err := appendPlanEvent(ctx, store, &event, "key-1")
	if err != nil {
		t.Fatalf("AppendPlanEvent first: %v", err)
	}

	second, err := appendPlanEvent(ctx, store, &event, "key-1")
	if err != nil {
		t.Fatalf("AppendPlanEvent second: %v", err)
	}

	if first.EventID != second.EventID || first.Sequence != second.Sequence {
		t.Fatalf("events are not idempotent: %#v %#v", first, second)
	}

	if first.AccountID != spec.AccountID || first.ProjectID != spec.ProjectID {
		t.Fatalf("event scope = %s/%s, want %s/%s", first.AccountID, first.ProjectID, spec.AccountID, spec.ProjectID)
	}

	if _, err := agentos.MarshalPlanEvent(&first); err != nil {
		t.Fatalf("stored event is not valid public PlanEvent: %v", err)
	}

	events, err := store.ListPlanEvents(ctx, &agentos.PlanStreamScope{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}, 0)
	if err != nil {
		t.Fatalf("ListPlanEvents: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
}

func TestMemoryPlanStoreAppendPlanEventScopesIdempotencyKeyByPlan(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := NewMemoryPlanStore()

	for _, planID := range []string{Plan1, "plan-2"} {
		spec := agentos.RunPlanSpec{
			PlanID:         planID,
			AccountID:      "acct-" + planID,
			ProjectID:      "proj-" + planID,
			IdempotencyKey: planID + "-start",
		}
		status := agentos.RunPlanStatus{PlanID: planID, LifecycleState: agentos.PlanLifecycleRunning}

		createMemoryPlanStateForEventTest(ctx, t, store, &spec, &status)
	}

	first, err := appendPlanEvent(ctx, store, &agentos.PlanEvent{
		Event:  agentoscore.Event{EventType: agentoscore.EventPlanStarted},
		PlanID: Plan1,
	}, "shared-key")
	if err != nil {
		t.Fatalf("AppendPlanEvent first plan: %v", err)
	}

	second, err := appendPlanEvent(ctx, store, &agentos.PlanEvent{
		Event:  agentoscore.Event{EventType: agentoscore.EventPlanStarted},
		PlanID: "plan-2",
	}, "shared-key")
	if err != nil {
		t.Fatalf("AppendPlanEvent second plan: %v", err)
	}

	if first.Sequence != 1 || second.Sequence != 1 || first.EventID != Plan1_1 || second.EventID != "plan-2:1" {
		t.Fatalf("events = %#v %#v, want independent plan-scoped event identities", first, second)
	}
}

func TestMemoryPlanStoreAppendPlanEventAssignsStoreOwnedIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := scopedEventTestPlanSpec("plan-start-1")
	status := agentos.RunPlanStatus{PlanID: Plan1, LifecycleState: agentos.PlanLifecycleRunning}
	createMemoryPlanStateForEventTest(
		ctx, t, store,
		&spec,
		&status,
	)

	event := agentos.PlanEvent{
		Event: agentoscore.Event{
			EventID:   "caller-event",
			EventType: agentoscore.EventPlanStarted,
			Sequence:  99,
		},
		PlanID: Plan1,
	}

	first, err := appendPlanEvent(ctx, store, &event, "event-key")
	if err != nil {
		t.Fatalf("AppendPlanEvent first: %v", err)
	}

	replay, err := appendPlanEvent(ctx, store, &event, "event-key")
	if err != nil {
		t.Fatalf("AppendPlanEvent replay: %v", err)
	}

	if first.EventID != Plan1_1 || first.Sequence != 1 {
		t.Fatalf("first event identity = %s/%d, want plan-1:1/1", first.EventID, first.Sequence)
	}

	if replay.EventID != first.EventID || replay.Sequence != first.Sequence {
		t.Fatalf("replay = %#v, want %#v", replay, first)
	}
}

func TestMemoryPlanStoreAppendPlanEventRequiresIdempotencyKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := scopedEventTestPlanSpec("plan-start-1")
	status := agentos.RunPlanStatus{PlanID: Plan1, LifecycleState: agentos.PlanLifecycleRunning}
	createMemoryPlanStateForEventTest(
		ctx, t, store,
		&spec,
		&status,
	)

	_, err := appendPlanEvent(ctx, store, &agentos.PlanEvent{
		Event:  agentoscore.Event{EventType: agentoscore.EventPlanStarted},
		PlanID: Plan1,
	}, "")
	if !errors.Is(err, agentoscore.ErrInvalidPlanEvent) {
		t.Fatalf("AppendPlanEvent empty key error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestMemoryPlanStoreAppendPlanEventRejectsMissingPlan(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()

	_, err := appendPlanEvent(context.Background(), store, &agentos.PlanEvent{
		Event:  agentoscore.Event{EventType: agentoscore.EventPlanStarted},
		PlanID: "missing-plan",
	}, "event-1")
	if !errors.Is(err, agentoscore.ErrPlanRouteNotFound) {
		t.Fatalf("AppendPlanEvent missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStoreAppendPlanEventRejectsDifferentReplay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := scopedEventTestPlanSpec("plan-start-1")
	status := agentos.RunPlanStatus{PlanID: Plan1, LifecycleState: agentos.PlanLifecycleRunning}
	createMemoryPlanStateForEventTest(ctx, t, store, &spec, &status)

	event := agentos.PlanEvent{
		Event:  agentoscore.Event{EventType: agentoscore.EventPlanStarted, Payload: map[string]any{"state": "started"}},
		PlanID: Plan1,
	}
	if _, err := appendPlanEvent(ctx, store, &event, "key-1"); err != nil {
		t.Fatalf("AppendPlanEvent first: %v", err)
	}

	event.EventType = agentoscore.EventPlanFailed

	event.Payload = map[string]any{"state": "failed"}
	if _, err := appendPlanEvent(ctx, store, &event, "key-1"); !errors.Is(err, agentoscore.ErrInvalidPlanEvent) {
		t.Fatalf("AppendPlanEvent changed replay error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestMemoryPlanStoreListPlanEventsEnforcesTenantScope(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{PlanID: Plan1, AccountID: "acct-1", ProjectID: "proj-1", IdempotencyKey: "plan-start-1"}
	status := agentos.RunPlanStatus{PlanID: Plan1, LifecycleState: agentos.PlanLifecycleRunning}
	createMemoryPlanStateForEventTest(ctx, t, store, &spec, &status)

	if _, err := appendPlanEvent(ctx, store, &agentos.PlanEvent{Event: agentoscore.Event{EventType: agentoscore.EventPlanStarted}, PlanID: spec.PlanID}, "event-1"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}

	for _, scope := range []agentos.PlanStreamScope{
		{PlanID: spec.PlanID},
		{PlanID: spec.PlanID, AccountID: spec.AccountID},
		{PlanID: spec.PlanID, ProjectID: spec.ProjectID},
	} {
		if _, err := store.ListPlanEvents(ctx, &scope, 0); !errors.Is(err, agentoscore.ErrInvalidPlanScope) {
			t.Fatalf("ListPlanEvents scope %#v error = %v, want ErrInvalidPlanScope", scope, err)
		}
	}

	if _, err := store.ListPlanEvents(ctx, &agentos.PlanStreamScope{PlanID: spec.PlanID, AccountID: "acct-other", ProjectID: spec.ProjectID}, 0); !errors.Is(err, agentoscore.ErrPlanRouteNotFound) {
		t.Fatalf("ListPlanEvents mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	events, err := store.ListPlanEvents(ctx, &agentos.PlanStreamScope{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}, 0)
	if err != nil {
		t.Fatalf("ListPlanEvents scoped: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
}

func TestPlanEventFromRetryScheduledIncludesAttempt(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	spec := scopedEventTestPlanSpec("")

	event, _, err := planEventFromStateEventForTest(
		&spec,
		&agentos.RunPlanStatus{PlanID: Plan1, LifecycleState: agentos.PlanLifecycleRunning, UpdatedAt: at},
		&StateEvent{
			Kind:    EventNodeRetryScheduled,
			NodeID:  Node1,
			RunID:   Run1,
			Reason:  "failed",
			Attempt: 2,
			At:      at,
		},
	)
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent: %v", err)
	}

	if event.EventType != agentoscore.EventPlanNodeRetryScheduled {
		t.Fatalf("event type = %q", event.EventType)
	}

	if event.Payload["attempt"] != int32(2) {
		t.Fatalf("payload = %#v", event.Payload)
	}
}

func TestPlanEventFromApprovalSignals(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		kind EventKind
		want agentoscore.EventType
	}{
		{name: "approved", kind: EventPlanApproved, want: agentoscore.EventPlanApproved},
		{name: "rejected", kind: EventPlanRejected, want: agentoscore.EventPlanRejected},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := scopedEventTestPlanSpec("")

			event, _, err := planEventFromStateEventForTest(
				&spec,
				&agentos.RunPlanStatus{PlanID: Plan1, LifecycleState: agentos.PlanLifecycleRunning, UpdatedAt: at},
				&StateEvent{Kind: tt.kind, Reason: "operator decision", At: at},
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
	t.Parallel()

	at := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	spec := scopedEventTestPlanSpec("")

	event, _, err := planEventFromStateEventForTest(
		&spec,
		&agentos.RunPlanStatus{
			PlanID:         Plan1,
			LifecycleState: agentos.PlanLifecycleRunning,
			BudgetUsage:    agentos.PlanBudgetUsage{SpentCents: 75},
			UpdatedAt:      at,
		},
		&StateEvent{
			Kind:        EventBudgetReported,
			NodeID:      Node1,
			RunID:       Run1,
			BudgetDelta: agentos.PlanBudgetUsage{SpentCents: 25},
			At:          at,
		},
	)
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent: %v", err)
	}

	if event.EventType != agentoscore.EventUsageReported {
		t.Fatalf("event type = %q", event.EventType)
	}

	if event.Payload["budget_delta"] == nil || event.Payload["budget_usage"] == nil {
		t.Fatalf("payload missing budget usage = %#v", event.Payload)
	}
}

func TestPlanEventFromDebugTraceEvents(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	spec := scopedEventTestPlanSpec("")
	status := agentos.RunPlanStatus{PlanID: Plan1, LifecycleState: agentos.PlanLifecycleRunning, UpdatedAt: at}

	capabilityEvent, _, err := planEventFromStateEventForTest(&spec, &status, &StateEvent{
		Kind:                   EventCapabilitySelected,
		NodeID:                 Node1,
		Capability:             CapabilitySelectionTrace{Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}, Capability: "run", HasOutputSchema: true},
		PreviousLifecycleState: agentos.PlanNodePending,
		NextLifecycleState:     agentos.PlanNodePending,
		At:                     at,
	})
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent capability: %v", err)
	}

	requirePlanEventPayload(t, &capabilityEvent, agentoscore.EventCapabilitySelected, planEventPayloadCapability)
	requirePlanEventPayload(t, &capabilityEvent, agentoscore.EventCapabilitySelected, planEventPayloadTransition)

	inputEvent, _, err := planEventFromStateEventForTest(&spec, &status, &StateEvent{
		Kind:   EventNodeInputResolved,
		NodeID: Node1,
		RunID:  Run1,
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

	requirePlanEventPayload(t, &inputEvent, agentoscore.EventNodeInputResolved, planEventPayloadInputResolution)

	conditionEvent, _, err := planEventFromStateEventForTest(&spec, &status, &StateEvent{
		Kind: EventConditionsEvaluated,
		ConditionTraces: []ConditionEvaluationTrace{
			{Scope: "node", NodeID: Node1, Expression: "inputs.enabled", Result: true},
		},
		At: at,
	})
	if err != nil {
		t.Fatalf("PlanEventFromStateEvent conditions: %v", err)
	}

	requirePlanEventPayload(t, &conditionEvent, agentoscore.EventConditionEvaluated, planEventPayloadConditions)
}

func requirePlanEventPayload(t *testing.T, event *agentos.PlanEvent, eventType agentoscore.EventType, payloadKey string) {
	t.Helper()

	if event.EventType != eventType {
		t.Fatalf("event type = %q, want %q", event.EventType, eventType)
	}

	if event.Payload[payloadKey] == nil {
		t.Fatalf("event missing payload %q: %#v", payloadKey, event.Payload)
	}
}

func TestNodeStartIdempotencyKeyIncludesAttempt(t *testing.T) {
	t.Parallel()

	first, err := NodeStartIdempotencyKey(Plan1, Node1, 1)
	if err != nil {
		t.Fatalf("NodeStartIdempotencyKey first: %v", err)
	}

	second, err := NodeStartIdempotencyKey(Plan1, Node1, 2)
	if err != nil {
		t.Fatalf("NodeStartIdempotencyKey second: %v", err)
	}

	if first == second {
		t.Fatalf("attempt-specific keys should differ: %q", first)
	}

	if _, err := NodeStartIdempotencyKey(Plan1, Node1, 0); err == nil {
		t.Fatal("NodeStartIdempotencyKey accepted attempt 0")
	}
}

func TestNodeTimeoutControlIdempotencyKeyRequiresRunID(t *testing.T) {
	t.Parallel()

	key, err := NodeTimeoutControlIdempotencyKey(Plan1, Node1, Run1)
	if err != nil {
		t.Fatalf("NodeTimeoutControlIdempotencyKey: %v", err)
	}

	if key == "" {
		t.Fatal("timeout control idempotency key is empty")
	}

	if _, err := NodeTimeoutControlIdempotencyKey(Plan1, Node1, ""); err == nil {
		t.Fatal("NodeTimeoutControlIdempotencyKey accepted empty run id")
	}
}

func TestPlanTimeoutControlIdempotencyKeyRequiresStartedAt(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)

	first, err := PlanTimeoutControlIdempotencyKey(Plan1, startedAt, 60)
	if err != nil {
		t.Fatalf("PlanTimeoutControlIdempotencyKey: %v", err)
	}

	second, err := PlanTimeoutControlIdempotencyKey(Plan1, startedAt, 60)
	if err != nil {
		t.Fatalf("PlanTimeoutControlIdempotencyKey replay: %v", err)
	}

	if first == "" || second != first {
		t.Fatalf("timeout control keys = %q/%q, want stable non-empty key", first, second)
	}

	if changed, err := PlanTimeoutControlIdempotencyKey(Plan1, startedAt.Add(time.Second), 60); err != nil || changed == first {
		t.Fatalf("changed started_at key = %q err=%v, want distinct key", changed, err)
	}

	if _, err := PlanTimeoutControlIdempotencyKey(Plan1, time.Time{}, 60); err == nil {
		t.Fatal("PlanTimeoutControlIdempotencyKey accepted empty start time")
	}

	if _, err := PlanTimeoutControlIdempotencyKey(Plan1, startedAt, 0); err == nil {
		t.Fatal("PlanTimeoutControlIdempotencyKey accepted empty timeout")
	}
}

func TestPlanSignalCancelControlIdempotencyKeyOnlyAcceptsReject(t *testing.T) {
	t.Parallel()

	reject := agentoscore.Signal{
		Type:           agentoscore.SignalPlanReject,
		IdempotencyKey: "reject-1",
		ActorID:        "operator-1",
	}

	first, err := PlanSignalCancelControlIdempotencyKey(Plan1, &reject)
	if err != nil {
		t.Fatalf("PlanSignalCancelControlIdempotencyKey: %v", err)
	}

	second, err := PlanSignalCancelControlIdempotencyKey(Plan1, &reject)
	if err != nil {
		t.Fatalf("PlanSignalCancelControlIdempotencyKey replay: %v", err)
	}

	if first == "" || first != second {
		t.Fatalf("signal cancel keys = %q/%q, want stable non-empty key", first, second)
	}

	if _, err := PlanSignalCancelControlIdempotencyKey(Plan1, &agentoscore.Signal{
		Type:           agentoscore.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}); !errors.Is(err, agentoscore.ErrInvalidSignal) {
		t.Fatalf("approve signal key error = %v, want ErrInvalidSignal", err)
	}
}

func createMemoryPlanStateForEventTest(ctx context.Context, t *testing.T, store *MemoryPlanStore, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) {
	t.Helper()

	ensureEventTestPlanScope(spec)

	if _, _, err := store.CreatePlan(ctx, spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
}

func scopedEventTestPlanSpec(idempotencyKey string) agentos.RunPlanSpec {
	spec := agentos.RunPlanSpec{
		PlanID:         Plan1,
		IdempotencyKey: idempotencyKey,
	}
	ensureEventTestPlanScope(&spec)

	return spec
}

func ensureEventTestPlanScope(spec *agentos.RunPlanSpec) {
	if spec.AccountID == "" {
		spec.AccountID = "acct-" + spec.PlanID
	}

	if spec.ProjectID == "" {
		spec.ProjectID = "proj-" + spec.PlanID
	}
}
