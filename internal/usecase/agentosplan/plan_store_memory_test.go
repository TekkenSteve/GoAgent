package agentosplan

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

const (
	CommandFailed      = "command-failed"
	mutatedValue       = "mutated"
	storedValue        = "stored"
	originalValue      = "original"
	callerValue        = "caller"
	planCopyTopicKey   = "topic"
	planCopyNameKey    = "name"
	planCopyNestedKey  = "nested"
	planCopyPayloadKey = "value"
)

func TestMemoryPlanStoreCreatePlanRequiresIdempotencyKey(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}
	status := agentos.RunPlanStatus{}

	_, _, err := store.CreatePlan(context.Background(), &spec, &status)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreCreatePlanCanonicalizesRequestedAtPrecision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("plan-requested-at", "start-key")
	spec.RequestedAt = time.Date(2026, 6, 20, 12, 0, 0, 123456789, time.UTC)

	status := agentos.RunPlanStatus{PlanID: spec.PlanID}
	if _, _, err := store.CreatePlan(ctx, &spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	snapshot, exists, err := store.LoadPlanState(ctx, spec.PlanID)
	if err != nil {
		t.Fatalf("LoadPlanState: %v", err)
	}

	if !exists {
		t.Fatal("LoadPlanState did not find plan")
	}

	if snapshot.Spec.RequestedAt.Nanosecond() != 123457000 {
		t.Fatalf("requested_at = %s, want microsecond precision", snapshot.Spec.RequestedAt.Format(time.RFC3339Nano))
	}
}

func TestMemoryPlanStoreCreatePlanCopiesCallerOwnedData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("plan-copy", "copy-start")
	spec.Metadata = map[string]string{"owner": callerValue}
	spec.Inputs = map[string]any{planCopyTopicKey: map[string]any{planCopyNameKey: originalValue}}
	spec.Nodes[0].Run.Input = map[string]any{"prompt": originalValue}

	status := agentos.RunPlanStatus{
		PlanID:       spec.PlanID,
		Metadata:     map[string]string{"state": originalValue},
		ActiveRunIDs: []string{"run-1"},
	}
	if _, _, err := store.CreatePlan(ctx, &spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	spec.Metadata["owner"] = mutatedValue
	requireAnyMap(t, spec.Inputs[planCopyTopicKey])[planCopyNameKey] = mutatedValue
	spec.Nodes[0].Run.Input["prompt"] = mutatedValue
	status.Metadata["state"] = mutatedValue
	status.ActiveRunIDs[0] = mutatedValue

	gotSpec, gotStatus, exists, err := store.GetPlan(ctx, spec.PlanID)
	if err != nil || !exists {
		t.Fatalf("GetPlan exists=%v err=%v", exists, err)
	}

	if gotSpec.Metadata["owner"] != callerValue ||
		requireAnyMap(t, gotSpec.Inputs[planCopyTopicKey])[planCopyNameKey] != originalValue ||
		gotSpec.Nodes[0].Run.Input["prompt"] != originalValue ||
		gotStatus.Metadata["state"] != originalValue ||
		gotStatus.ActiveRunIDs[0] != "run-1" {
		t.Fatalf("stored plan was mutated: spec=%#v status=%#v", gotSpec, gotStatus)
	}
}

func requireAnyMap(t *testing.T, value any) map[string]any {
	t.Helper()

	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value = %#v, want map[string]any", value)
	}

	return object
}

func TestMemoryPlanStoreSavePlanStateRequiresIdempotencyKey(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()

	err := savePlanState(context.Background(), store, &PlanStateSnapshot{
		Spec:   agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"},
		Status: agentos.RunPlanStatus{PlanID: "plan-1"},
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreSavePlanStateCopiesCallerOwnedData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(ctx, t, store, "plan-save-copy")

	snapshot := PlanStateSnapshot{
		Spec: spec,
		Status: agentos.RunPlanStatus{
			PlanID:   spec.PlanID,
			Metadata: map[string]string{"state": storedValue},
			Nodes: []agentos.PlanNodeStatus{
				{NodeID: "node-1", RunID: "run-1", Artifacts: []agentos.ArtifactRef{{ArtifactID: "artifact-1", Metadata: map[string]string{"kind": storedValue}}}},
			},
		},
	}
	if err := savePlanState(ctx, store, &snapshot); err != nil {
		t.Fatalf("SavePlanState: %v", err)
	}

	snapshot.Status.Metadata["state"] = mutatedValue
	snapshot.Status.Nodes[0].Artifacts[0].Metadata["kind"] = mutatedValue

	stored, exists, err := store.LoadPlanState(ctx, spec.PlanID)
	if err != nil || !exists {
		t.Fatalf("LoadPlanState exists=%v err=%v", exists, err)
	}

	if stored.Status.Metadata["state"] != storedValue || stored.Status.Nodes[0].Artifacts[0].Metadata["kind"] != storedValue {
		t.Fatalf("stored snapshot was mutated: %#v", stored.Status)
	}
}

func TestMemoryPlanStoreSavePlanStateRejectsMissingPlan(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("missing-plan", "missing-plan-start-key")

	err := savePlanState(context.Background(), store, &PlanStateSnapshot{
		Spec:   spec,
		Status: agentos.RunPlanStatus{PlanID: spec.PlanID, LifecycleState: agentos.PlanLifecycleRunning},
	})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("SavePlanState missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStorePersistPlanTransitionRejectsMissingPlan(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("missing-plan", "missing-plan-start-key")

	_, err := persistPlanTransition(context.Background(), store, &PlanStateSnapshot{
		Spec:   spec,
		Status: agentos.RunPlanStatus{PlanID: spec.PlanID, LifecycleState: agentos.PlanLifecycleRunning},
	}, &agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: spec.PlanID,
	}, "missing-plan-event")
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("PersistPlanTransition missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStoreAppendPlanEventCopiesPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(ctx, t, store, "plan-event-copy")

	event := agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted, Payload: map[string]any{planCopyNestedKey: map[string]any{planCopyPayloadKey: storedValue}}},
		PlanID: spec.PlanID,
	}
	if _, err := appendPlanEvent(ctx, store, &event, "event-copy"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}

	requireNestedPayload(t, event.Payload)[planCopyPayloadKey] = mutatedValue

	events, err := store.ListPlanEvents(ctx, &agentos.PlanStreamScope{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}, 0)
	if err != nil {
		t.Fatalf("ListPlanEvents: %v", err)
	}

	if len(events) != 1 || requireNestedPayload(t, events[0].Payload)[planCopyPayloadKey] != storedValue {
		t.Fatalf("events = %#v", events)
	}
}

func requireNestedPayload(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()

	return requireAnyMap(t, payload[planCopyNestedKey])
}

func TestMemoryPlanStorePersistPlanTransitionIsAtomic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("plan-1", "start-key")

	initial := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
	}
	if _, _, err := store.CreatePlan(ctx, &spec, &initial); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	firstEvent := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanStarted,
			Payload:   map[string]any{"state": "running"},
		},
		PlanID: spec.PlanID,
	}
	if _, err := appendPlanEvent(ctx, store, &firstEvent, "transition-key"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}

	next := initial
	next.LifecycleState = agentos.PlanLifecycleFailed
	next.Reason = "should not commit"
	changedEvent := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanFailed,
			Payload:   map[string]any{"state": "failed"},
		},
		PlanID: spec.PlanID,
	}

	_, err := persistPlanTransition(ctx, store, &PlanStateSnapshot{
		Spec:   spec,
		Status: next,
	}, &changedEvent, "transition-key")
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("PersistPlanTransition error = %v, want ErrInvalidPlanEvent", err)
	}

	snapshot, exists, err := store.LoadPlanState(ctx, spec.PlanID)
	if err != nil || !exists {
		t.Fatalf("LoadPlanState exists=%v err=%v", exists, err)
	}

	if snapshot.Status.LifecycleState != agentos.PlanLifecycleRunning || snapshot.Status.Reason != "" {
		t.Fatalf("snapshot status = %#v, want original running state", snapshot.Status)
	}
}

func TestMemoryPlanStorePersistPlanTransitionRejectsEventOnlyIdempotencyKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("plan-1", "start-key")

	initial := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
	}
	if _, _, err := store.CreatePlan(ctx, &spec, &initial); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	event := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanStarted,
			Payload:   map[string]any{"state": "running"},
		},
		PlanID: spec.PlanID,
	}
	if _, err := appendPlanEvent(ctx, store, &event, "event-only-key"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}

	_, err := persistPlanTransition(ctx, store, &PlanStateSnapshot{
		Spec:   spec,
		Status: initial,
	}, &event, "event-only-key")
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("PersistPlanTransition event-only key error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStorePersistPlanTransitionRejectsSnapshotReplayMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("plan-1", "start-key")

	initial := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecyclePending,
		UpdatedAt:      time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
	}
	if _, _, err := store.CreatePlan(ctx, &spec, &initial); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	running := initial
	running.LifecycleState = agentos.PlanLifecycleRunning

	event := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanStarted,
			Payload:   map[string]any{"state": "running"},
		},
		PlanID: spec.PlanID,
	}
	if _, err := persistPlanTransition(ctx, store, &PlanStateSnapshot{
		Spec:   spec,
		Status: running,
	}, &event, "transition-key"); err != nil {
		t.Fatalf("PersistPlanTransition first: %v", err)
	}

	changed := running
	changed.LifecycleState = agentos.PlanLifecycleFailed
	changed.Reason = "same event different snapshot"

	_, err := persistPlanTransition(ctx, store, &PlanStateSnapshot{
		Spec:   spec,
		Status: changed,
	}, &event, "transition-key")
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("PersistPlanTransition replay mismatch error = %v, want ErrInvalidRunPlan", err)
	}

	snapshot, exists, err := store.LoadPlanState(ctx, spec.PlanID)
	if err != nil || !exists {
		t.Fatalf("LoadPlanState exists=%v err=%v", exists, err)
	}

	if snapshot.Status.LifecycleState != agentos.PlanLifecycleRunning || snapshot.Status.Reason != "" {
		t.Fatalf("snapshot status = %#v, want original running state", snapshot.Status)
	}
}

func TestMemoryPlanStoreGetPlanByRefRequiresTenantScope(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(ctx, t, store, "plan-1")

	for _, ref := range []agentos.PlanRef{
		{PlanID: spec.PlanID},
		{PlanID: spec.PlanID, AccountID: spec.AccountID},
		{PlanID: spec.PlanID, ProjectID: spec.ProjectID},
	} {
		if _, _, _, err := store.GetPlanByRef(ctx, ref); !errors.Is(err, agentos.ErrInvalidPlanScope) {
			t.Fatalf("GetPlanByRef ref %#v error = %v, want ErrInvalidPlanScope", ref, err)
		}
	}

	if _, _, _, err := store.GetPlanByRef(ctx, agentos.PlanRef{PlanID: spec.PlanID, AccountID: "acct-other", ProjectID: spec.ProjectID}); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("GetPlanByRef tenant mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	loaded, status, exists, err := store.GetPlanByRef(ctx, agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID})
	if err != nil {
		t.Fatalf("GetPlanByRef scoped: %v", err)
	}

	if !exists || loaded.PlanID != spec.PlanID || status.PlanID != spec.PlanID {
		t.Fatalf("GetPlanByRef = spec=%#v status=%#v exists=%v", loaded, status, exists)
	}
}

func TestMemoryPlanStoreRecordAuditRejectsMissingPlan(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()

	missingAudit := AuditRecord{
		PlanID:         "missing-plan",
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-1",
	}
	_, _, err := store.RecordAudit(context.Background(), &missingAudit)

	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("RecordAudit missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStoreRecordPlanCommandRejectsMissingPlan(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()

	missingCommand := PlanCommandRecord{
		PlanID:         "missing-plan",
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-1",
	}
	_, _, err := store.RecordPlanCommand(context.Background(), &missingCommand)

	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("RecordPlanCommand missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStoreSavePlanMetricCheckpointRejectsMissingPlan(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()

	err := savePlanMetricCheckpoint(context.Background(), store, &PlanMetricCheckpoint{
		ExporterID: "exporter-1",
		PlanID:     "missing-plan",
		AccountID:  "acct-1",
		ProjectID:  "proj-1",
		Sequence:   1,
	})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("SavePlanMetricCheckpoint missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStoreSavePlanMetricCheckpointRejectsTenantMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(ctx, t, store, "plan-1")

	err := savePlanMetricCheckpoint(ctx, store, &PlanMetricCheckpoint{
		ExporterID: "exporter-1",
		PlanID:     spec.PlanID,
		AccountID:  "acct-other",
		ProjectID:  spec.ProjectID,
		Sequence:   1,
	})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("SavePlanMetricCheckpoint tenant mismatch error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStoreCreatePlanIsIdempotentForSameRequest(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("plan-1", "start-key")
	status := agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecyclePending}

	first, created, err := store.CreatePlan(context.Background(), &spec, &status)
	if err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}

	if !created {
		t.Fatal("first CreatePlan was not created")
	}

	replayStatus := agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning}

	second, created, err := store.CreatePlan(context.Background(), &spec, &replayStatus)
	if err != nil {
		t.Fatalf("CreatePlan second: %v", err)
	}

	if created {
		t.Fatal("second CreatePlan created a duplicate plan")
	}

	if second.LifecycleState != first.LifecycleState {
		t.Fatalf("second status = %#v, want %#v", second, first)
	}
}

func TestMemoryPlanStoreCreatePlanRejectsReusedKeyForDifferentRequest(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()

	spec := testRunPlanSpec("plan-1", "start-key")
	status := agentos.RunPlanStatus{PlanID: "plan-1"}

	if _, _, err := store.CreatePlan(context.Background(), &spec, &status); err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}

	changed := testRunPlanSpec("plan-1", "start-key")
	changed.Nodes[0].NodeID = "changed"

	changedStatus := agentos.RunPlanStatus{PlanID: "plan-1"}

	_, _, err := store.CreatePlan(context.Background(), &changed, &changedStatus)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreCreatePlanAllowsReusedKeyAcrossTenantScope(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	first := testRunPlanSpec("plan-1", "start-key")

	firstStatus := agentos.RunPlanStatus{PlanID: "plan-1"}
	if _, _, err := store.CreatePlan(context.Background(), &first, &firstStatus); err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}

	second := testRunPlanSpec("plan-2", "start-key")
	secondStatus := agentos.RunPlanStatus{PlanID: "plan-2"}

	_, _, err := store.CreatePlan(context.Background(), &second, &secondStatus)
	if err != nil {
		t.Fatalf("CreatePlan different tenant: %v", err)
	}
}

func TestMemoryPlanStoreCreatePlanRejectsReusedKeyWithinTenantScope(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()

	first := testRunPlanSpec("plan-1", "start-key")

	firstStatus := agentos.RunPlanStatus{PlanID: first.PlanID}
	if _, _, err := store.CreatePlan(context.Background(), &first, &firstStatus); err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}

	second := testRunPlanSpec("plan-2", "start-key")
	second.AccountID = first.AccountID
	second.ProjectID = first.ProjectID

	secondStatus := agentos.RunPlanStatus{PlanID: second.PlanID}

	_, _, err := store.CreatePlan(context.Background(), &second, &secondStatus)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreCreatePlanRejectsPlanIDWithDifferentKey(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	first := testRunPlanSpec("plan-1", "start-key-1")
	firstStatus := agentos.RunPlanStatus{PlanID: "plan-1"}

	if _, _, err := store.CreatePlan(context.Background(), &first, &firstStatus); err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}

	second := testRunPlanSpec("plan-1", "start-key-2")
	secondStatus := agentos.RunPlanStatus{PlanID: "plan-1"}

	_, _, err := store.CreatePlan(context.Background(), &second, &secondStatus)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreGetAuditRecord(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(context.Background(), t, store, "plan-1")
	record := AuditRecord{
		PlanID:         spec.PlanID,
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-1",
	}

	stored, _, err := store.RecordAudit(context.Background(), &record)
	if err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}

	got, exists, err := store.GetAuditRecord(context.Background(), AuditRefFromRecord(&stored))
	if err != nil {
		t.Fatalf("GetAuditRecord: %v", err)
	}

	if !exists ||
		got.PlanID != record.PlanID ||
		got.AccountID != spec.AccountID ||
		got.ProjectID != spec.ProjectID ||
		got.Action != record.Action {
		t.Fatalf("audit = %#v exists=%v", got, exists)
	}
}

func TestMemoryPlanStoreRecordAuditDoesNotMutateCallerRecord(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(context.Background(), t, store, "plan-audit-copy")
	record := AuditRecord{
		PlanID:         spec.PlanID,
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-copy",
		Payload:        map[string]any{planCopyNestedKey: map[string]any{planCopyPayloadKey: storedValue}},
	}

	stored, _, err := store.RecordAudit(context.Background(), &record)
	if err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}

	if record.AccountID != "" || record.ProjectID != "" || record.AuditID != "" || !record.CreatedAt.IsZero() {
		t.Fatalf("caller record was mutated: %#v", record)
	}

	requireNestedPayload(t, record.Payload)[planCopyPayloadKey] = mutatedValue

	got, exists, err := store.GetAuditRecord(context.Background(), AuditRefFromRecord(&stored))
	if err != nil || !exists {
		t.Fatalf("GetAuditRecord exists=%v err=%v", exists, err)
	}

	if requireNestedPayload(t, got.Payload)[planCopyPayloadKey] != storedValue {
		t.Fatalf("stored audit payload was mutated: %#v", got.Payload)
	}
}

func TestMemoryPlanStoreRejectsAuditKeyReuseWithDifferentRequest(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(context.Background(), t, store, "plan-1")

	record := AuditRecord{
		PlanID:         spec.PlanID,
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}
	if _, _, err := store.RecordAudit(context.Background(), &record); err != nil {
		t.Fatalf("RecordAudit first: %v", err)
	}

	changed := record
	changed.Payload = map[string]any{"type": string(agentos.SignalPlanReject)}

	_, _, err := store.RecordAudit(context.Background(), &changed)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreAllowsAuditKeyReuseAcrossPlans(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	first := createMemoryPlanForTest(context.Background(), t, store, "plan-1")
	second := createMemoryPlanForTest(context.Background(), t, store, "plan-2")

	firstAudit := AuditRecord{
		PlanID:         first.PlanID,
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}
	if _, _, err := store.RecordAudit(context.Background(), &firstAudit); err != nil {
		t.Fatalf("RecordAudit first: %v", err)
	}

	secondAudit := AuditRecord{
		PlanID:         second.PlanID,
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanReject)},
	}
	if _, _, err := store.RecordAudit(context.Background(), &secondAudit); err != nil {
		t.Fatalf("RecordAudit second: %v", err)
	}
}

func TestMemoryPlanStoreRecordAuditRejectsInvalidNodeRunOwnership(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(context.Background(), t, store, "plan-1")

	valid := AuditRecord{
		PlanID:         spec.PlanID,
		NodeID:         "node-1",
		RunID:          "run-1",
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "valid-node-audit",
	}
	if _, _, err := store.RecordAudit(context.Background(), &valid); err != nil {
		t.Fatalf("RecordAudit valid node/run: %v", err)
	}

	unpaired := valid
	unpaired.IdempotencyKey = "unpaired-node-audit"

	unpaired.RunID = ""
	if _, _, err := store.RecordAudit(context.Background(), &unpaired); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("RecordAudit unpaired node/run error = %v, want ErrInvalidRunPlan", err)
	}

	missingNode := valid
	missingNode.IdempotencyKey = "missing-node-audit"

	missingNode.NodeID = "missing-node"
	if _, _, err := store.RecordAudit(context.Background(), &missingNode); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("RecordAudit missing node error = %v, want ErrInvalidRunPlan", err)
	}

	wrongRun := valid
	wrongRun.IdempotencyKey = "wrong-run-audit"

	wrongRun.RunID = "run-other"
	if _, _, err := store.RecordAudit(context.Background(), &wrongRun); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("RecordAudit wrong run error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStorePlanCommandLifecycleIsIdempotent(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(context.Background(), t, store, "plan-1")
	command := memoryPlanControlCommand(spec.PlanID, "control-1")

	first, created := recordPlanCommandForMemoryTest(context.Background(), t, store, &command, "first")
	requireCreatedPendingCommand(t, &first, created)
	requirePlanCommandScope(t, &first, &spec)

	replay, created := recordPlanCommandForMemoryTest(context.Background(), t, store, &command, "replay")
	requirePlanCommandReplay(t, &replay, created, &first)

	deliverPlanCommandForMemoryTest(context.Background(), t, store, &first)
	requireDeliveredPlanCommandRejectsFailure(context.Background(), t, store, &first)
}

func TestMemoryPlanStoreRecordPlanCommandDoesNotMutateCallerCommand(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(context.Background(), t, store, "plan-command-copy")
	command := PlanCommandRecord{
		PlanID:         spec.PlanID,
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-copy",
		Payload:        map[string]any{planCopyNestedKey: map[string]any{planCopyPayloadKey: storedValue}},
	}

	stored, _, err := store.RecordPlanCommand(context.Background(), &command)
	if err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	requireCallerCommandUnchanged(t, &command)

	requireNestedPayload(t, command.Payload)[planCopyPayloadKey] = mutatedValue

	got, exists, err := store.GetPlanCommand(context.Background(), PlanCommandRefFromRecord(&stored))
	if err != nil || !exists {
		t.Fatalf("GetPlanCommand exists=%v err=%v", exists, err)
	}

	requireStoredCommandPayload(t, &got)
}

func requireCallerCommandUnchanged(t *testing.T, command *PlanCommandRecord) {
	t.Helper()

	if command.AccountID != "" || command.ProjectID != "" || command.CommandID != "" || command.Status != "" || !command.CreatedAt.IsZero() || !command.UpdatedAt.IsZero() {
		t.Fatalf("caller command was mutated: %#v", command)
	}
}

func requireStoredCommandPayload(t *testing.T, command *PlanCommandRecord) {
	t.Helper()

	if requireNestedPayload(t, command.Payload)[planCopyPayloadKey] != storedValue {
		t.Fatalf("stored command payload was mutated: %#v", command.Payload)
	}
}

func TestMemoryPlanStoreListRecoverablePlanCommands(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(ctx, t, store, "plan-1")
	otherSpec := createMemoryPlanForTest(ctx, t, store, "plan-2")

	commands := seedRecoverablePlanCommands(ctx, t, store, spec.PlanID, otherSpec.PlanID)
	advanceRecoverablePlanCommands(ctx, t, store, commands)

	got, err := store.ListRecoverablePlanCommands(ctx, &PlanCommandScope{
		PlanID:   spec.PlanID,
		Action:   AuditActionPlanSignal,
		Statuses: []PlanCommandStatus{PlanCommandPending},
		Limit:    1,
	})
	if err != nil {
		t.Fatalf("ListRecoverablePlanCommands: %v", err)
	}

	if len(got) != 1 || got[0].CommandID != "command-pending" {
		t.Fatalf("commands = %#v", got)
	}

	requireRecoverableCommands(ctx, t, store, &PlanCommandScope{PlanID: spec.PlanID, Statuses: []PlanCommandStatus{PlanCommandFailed}}, CommandFailed)
	requireRecoverableCommands(ctx, t, store, &PlanCommandScope{
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Statuses:  []PlanCommandStatus{PlanCommandFailed},
	}, CommandFailed)
}

func TestRecoverablePlanCommandStatusesRejectsDelivered(t *testing.T) {
	t.Parallel()

	scope := PlanCommandScope{
		Statuses: []PlanCommandStatus{PlanCommandDelivered},
	}
	requireRecoverablePlanCommandStatusesError(t, &scope, agentos.ErrInvalidRunPlan)
}

func requireRecoverablePlanCommandStatusesError(t *testing.T, scope *PlanCommandScope, want error) {
	t.Helper()

	_, err := RecoverablePlanCommandStatuses(scope)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestRecoverablePlanCommandStatusesRejectsPartialTenantScope(t *testing.T) {
	t.Parallel()

	for _, scope := range []PlanCommandScope{
		{AccountID: "acct-1"},
		{ProjectID: "proj-1"},
	} {
		_, err := RecoverablePlanCommandStatuses(&scope)
		if !errors.Is(err, agentos.ErrInvalidPlanScope) {
			t.Fatalf("RecoverablePlanCommandStatuses(%#v) error = %v, want ErrInvalidPlanScope", scope, err)
		}
	}
}

func TestMemoryPlanStoreRejectsCommandKeyReuseWithDifferentRequest(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(context.Background(), t, store, "plan-1")

	command := PlanCommandRecord{
		PlanID:         spec.PlanID,
		ActorID:        "operator-1",
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}
	if _, _, err := store.RecordPlanCommand(context.Background(), &command); err != nil {
		t.Fatalf("RecordPlanCommand first: %v", err)
	}

	changed := command
	changed.Payload = map[string]any{"type": string(agentos.SignalPlanReject)}

	_, _, err := store.RecordPlanCommand(context.Background(), &changed)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreAllowsCommandKeyReuseAcrossPlans(t *testing.T) {
	t.Parallel()

	store := NewMemoryPlanStore()
	first := createMemoryPlanForTest(context.Background(), t, store, "plan-1")
	second := createMemoryPlanForTest(context.Background(), t, store, "plan-2")

	firstCommand := PlanCommandRecord{
		PlanID:         first.PlanID,
		ActorID:        "operator-1",
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}
	if _, _, err := store.RecordPlanCommand(context.Background(), &firstCommand); err != nil {
		t.Fatalf("RecordPlanCommand first: %v", err)
	}

	secondCommand := PlanCommandRecord{
		PlanID:         second.PlanID,
		ActorID:        "operator-1",
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanReject)},
	}
	if _, _, err := store.RecordPlanCommand(context.Background(), &secondCommand); err != nil {
		t.Fatalf("RecordPlanCommand second: %v", err)
	}
}

func TestMemoryPlanStoreListAuditRecordsFiltersAndLimits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()

	spec := testRunPlanSpec("plan-1", "start-1")
	status := NewState(&spec, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)).Status

	if _, _, err := store.CreatePlan(ctx, &spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	seedAuditRecordsForMemoryTest(ctx, t, store, spec.PlanID)

	got, err := store.ListAuditRecords(ctx, &agentos.PlanAuditScope{
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		NodeID:    "node-1",
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("ListAuditRecords: %v", err)
	}

	if len(got) != 1 || got[0].AuditID != "audit-1" {
		t.Fatalf("audits = %#v", got)
	}

	for _, scope := range []agentos.PlanAuditScope{
		{PlanID: spec.PlanID},
		{PlanID: spec.PlanID, AccountID: spec.AccountID},
		{PlanID: spec.PlanID, ProjectID: spec.ProjectID},
	} {
		if _, err := store.ListAuditRecords(ctx, &scope); !errors.Is(err, agentos.ErrInvalidPlanScope) {
			t.Fatalf("ListAuditRecords scope %#v error = %v, want ErrInvalidPlanScope", scope, err)
		}
	}

	if _, err := store.ListAuditRecords(ctx, &agentos.PlanAuditScope{PlanID: spec.PlanID, AccountID: "acct-other", ProjectID: spec.ProjectID}); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("tenant mismatch error = %v, want ErrPlanRouteNotFound", err)
	}
}

func testRunPlanSpec(planID, idempotencyKey string) agentos.RunPlanSpec {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}

	return agentos.RunPlanSpec{
		PlanID:         planID,
		AccountID:      "acct-" + planID,
		ProjectID:      "proj-" + planID,
		IdempotencyKey: idempotencyKey,
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "node-1", Run: agentos.RunSpec{RunID: "run-1", Backend: ref}},
		},
	}
}

func createMemoryPlanForTest(ctx context.Context, t *testing.T, store *MemoryPlanStore, planID string) agentos.RunPlanSpec {
	t.Helper()

	spec := testRunPlanSpec(planID, planID+"-start-key")

	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		Nodes:          NewState(&spec, time.Now().UTC()).Status.Nodes,
	}
	if _, _, err := store.CreatePlan(ctx, &spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	return spec
}

func memoryPlanControlCommand(planID, idempotencyKey string) PlanCommandRecord {
	return PlanCommandRecord{
		PlanID:         planID,
		ActorID:        "operator-1",
		Action:         AuditActionPlanControl,
		IdempotencyKey: idempotencyKey,
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
	}
}

func recordPlanCommandForMemoryTest(ctx context.Context, t *testing.T, store *MemoryPlanStore, command *PlanCommandRecord, label string) (PlanCommandRecord, bool) {
	t.Helper()

	stored, created, err := store.RecordPlanCommand(ctx, command)
	if err != nil {
		t.Fatalf("RecordPlanCommand %s: %v", label, err)
	}

	return stored, created
}

func requireCreatedPendingCommand(t *testing.T, command *PlanCommandRecord, created bool) {
	t.Helper()

	if !created || command.Status != PlanCommandPending {
		t.Fatalf("first command = %#v created=%v", command, created)
	}
}

func requirePlanCommandScope(t *testing.T, command *PlanCommandRecord, spec *agentos.RunPlanSpec) {
	t.Helper()

	if command.AccountID != spec.AccountID || command.ProjectID != spec.ProjectID {
		t.Fatalf("command scope = %s/%s, want %s/%s", command.AccountID, command.ProjectID, spec.AccountID, spec.ProjectID)
	}
}

func requirePlanCommandReplay(t *testing.T, replay *PlanCommandRecord, created bool, first *PlanCommandRecord) {
	t.Helper()

	if created {
		t.Fatal("replay created a new command, expected idempotent no-op")
	}

	if replay.CommandID != first.CommandID {
		t.Fatalf("replay.CommandID = %q, want %q", replay.CommandID, first.CommandID)
	}

	if replay.Status != first.Status {
		t.Fatalf("replay.Status = %q, want %q", replay.Status, first.Status)
	}

	if replay.PlanID != first.PlanID {
		t.Fatalf("replay.PlanID = %q, want %q", replay.PlanID, first.PlanID)
	}

	if replay.IdempotencyKey != first.IdempotencyKey {
		t.Fatalf("replay.IdempotencyKey = %q, want %q", replay.IdempotencyKey, first.IdempotencyKey)
	}
}

func deliverPlanCommandForMemoryTest(ctx context.Context, t *testing.T, store *MemoryPlanStore, command *PlanCommandRecord) {
	t.Helper()

	deliveredAudit := AuditRecordFromPlanCommand(command)
	if _, _, err := store.RecordAudit(ctx, &deliveredAudit); err != nil {
		t.Fatalf("RecordAudit for delivered command: %v", err)
	}

	if _, err := store.MarkPlanCommandDelivered(ctx, PlanCommandRefFromRecord(command)); err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}
}

func requireDeliveredPlanCommandRejectsFailure(ctx context.Context, t *testing.T, store *MemoryPlanStore, command *PlanCommandRecord) {
	t.Helper()

	if _, err := store.MarkPlanCommandFailed(ctx, PlanCommandRefFromRecord(command), "should be rejected"); err == nil {
		t.Fatal("MarkPlanCommandFailed should reject a delivered command, but succeeded")
	}
}

func seedRecoverablePlanCommands(ctx context.Context, t *testing.T, store *MemoryPlanStore, planID, otherPlanID string) []PlanCommandRecord {
	t.Helper()

	commands := recoverablePlanCommandFixtures(planID, otherPlanID)
	for i := range commands {
		stored, _ := recordPlanCommandForMemoryTest(ctx, t, store, &commands[i], commands[i].CommandID)
		commands[i] = stored
	}

	return commands
}

func recoverablePlanCommandFixtures(planID, otherPlanID string) []PlanCommandRecord {
	return []PlanCommandRecord{
		memoryPlanCommandFixture("command-delivered", planID, AuditActionPlanControl, map[string]any{"operation": string(agentos.ControlCancel)}, 0),
		memoryPlanCommandFixture("command-pending", planID, AuditActionPlanSignal, map[string]any{"type": string(agentos.SignalPlanApprove)}, 1),
		memoryPlanCommandFixture(CommandFailed, planID, AuditActionPlanSignal, map[string]any{"type": string(agentos.SignalPlanReject)}, 2),
		memoryPlanCommandFixture("command-other-tenant", otherPlanID, AuditActionPlanSignal, map[string]any{"type": string(agentos.SignalPlanReject)}, 3),
	}
}

func memoryPlanCommandFixture(commandID, planID string, action AuditAction, payload map[string]any, minute int) PlanCommandRecord {
	at := time.Date(2026, 6, 19, 12, minute, 0, 0, time.UTC)

	return PlanCommandRecord{
		CommandID:      commandID,
		PlanID:         planID,
		Action:         action,
		IdempotencyKey: commandID,
		Payload:        payload,
		CreatedAt:      at,
		UpdatedAt:      at,
	}
}

func advanceRecoverablePlanCommands(ctx context.Context, t *testing.T, store *MemoryPlanStore, commands []PlanCommandRecord) {
	t.Helper()

	deliveredAudit := AuditRecordFromPlanCommand(&commands[0])
	if _, _, err := store.RecordAudit(ctx, &deliveredAudit); err != nil {
		t.Fatalf("RecordAudit delivered command: %v", err)
	}

	if _, err := store.MarkPlanCommandDelivered(ctx, PlanCommandRefFromRecord(&commands[0])); err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}

	if _, err := store.MarkPlanCommandFailed(ctx, PlanCommandRefFromRecord(&commands[2]), "temporal unavailable"); err != nil {
		t.Fatalf("MarkPlanCommandFailed: %v", err)
	}

	if _, err := store.MarkPlanCommandFailed(ctx, PlanCommandRefFromRecord(&commands[3]), "temporal unavailable"); err != nil {
		t.Fatalf("MarkPlanCommandFailed other tenant: %v", err)
	}
}

func requireRecoverableCommands(ctx context.Context, t *testing.T, store *MemoryPlanStore, scope *PlanCommandScope, wantCommandID string) {
	t.Helper()

	got, err := store.ListRecoverablePlanCommands(ctx, scope)
	if err != nil {
		t.Fatalf("ListRecoverablePlanCommands: %v", err)
	}

	if len(got) != 1 || got[0].CommandID != wantCommandID {
		t.Fatalf("commands = %#v, want %s", got, wantCommandID)
	}
}

func seedAuditRecordsForMemoryTest(ctx context.Context, t *testing.T, store *MemoryPlanStore, planID string) {
	t.Helper()

	records := memoryAuditRecordFixtures(planID)
	for i := range records {
		if _, _, err := store.RecordAudit(ctx, &records[i]); err != nil {
			t.Fatalf("RecordAudit %s: %v", records[i].AuditID, err)
		}
	}
}

func memoryAuditRecordFixtures(planID string) []AuditRecord {
	return []AuditRecord{
		memoryAuditRecordFixture("audit-2", planID, "node-1", "run-1", AuditActionPlanSignal, "signal-1", 1),
		memoryAuditRecordFixture("audit-1", planID, "node-1", "run-1", AuditActionPlanControl, "control-1", 0),
		memoryAuditRecordFixture("audit-plan-level", planID, "", "", AuditActionPlanControl, "control-2", 2),
	}
}

func memoryAuditRecordFixture(auditID, planID, nodeID, runID string, action AuditAction, idempotencyKey string, minute int) AuditRecord {
	return AuditRecord{
		AuditID:        auditID,
		PlanID:         planID,
		NodeID:         nodeID,
		RunID:          runID,
		Action:         action,
		IdempotencyKey: idempotencyKey,
		CreatedAt:      time.Date(2026, 6, 19, 12, minute, 0, 0, time.UTC),
	}
}
