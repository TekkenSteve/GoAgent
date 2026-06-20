package agentosplan

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestMemoryPlanStoreCreatePlanRequiresIdempotencyKey(t *testing.T) {
	store := NewMemoryPlanStore()
	_, _, err := store.CreatePlan(context.Background(), agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}, agentos.RunPlanStatus{})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreSavePlanStateRequiresIdempotencyKey(t *testing.T) {
	store := NewMemoryPlanStore()
	err := store.SavePlanState(context.Background(), PlanStateSnapshot{
		Spec:   agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"},
		Status: agentos.RunPlanStatus{PlanID: "plan-1"},
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreSavePlanStateRejectsMissingPlan(t *testing.T) {
	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("missing-plan", "missing-plan-start-key")

	err := store.SavePlanState(context.Background(), PlanStateSnapshot{
		Spec:           spec,
		Status:         agentos.RunPlanStatus{PlanID: spec.PlanID, LifecycleState: agentos.PlanLifecycleRunning},
		IdempotencyKey: spec.IdempotencyKey,
	})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("SavePlanState missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStorePersistPlanTransitionRejectsMissingPlan(t *testing.T) {
	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("missing-plan", "missing-plan-start-key")

	_, err := store.PersistPlanTransition(context.Background(), PlanStateSnapshot{
		Spec:           spec,
		Status:         agentos.RunPlanStatus{PlanID: spec.PlanID, LifecycleState: agentos.PlanLifecycleRunning},
		IdempotencyKey: spec.IdempotencyKey,
	}, agentos.PlanEvent{
		Event:  agentos.Event{EventType: agentos.EventPlanStarted},
		PlanID: spec.PlanID,
	}, "missing-plan-event")
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("PersistPlanTransition missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStorePersistPlanTransitionIsAtomic(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("plan-1", "start-key")
	initial := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
	}
	if _, _, err := store.CreatePlan(ctx, spec, initial); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
	firstEvent := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: agentos.EventPlanStarted,
			Payload:   map[string]any{"state": "running"},
		},
		PlanID: spec.PlanID,
	}
	if _, err := store.AppendPlanEvent(ctx, firstEvent, "transition-key"); err != nil {
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
	_, err := store.PersistPlanTransition(ctx, PlanStateSnapshot{
		Spec:           spec,
		Status:         next,
		IdempotencyKey: "transition-key",
	}, changedEvent, "transition-key")
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

func TestMemoryPlanStoreGetPlanByRefRequiresTenantScope(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(t, ctx, store, "plan-1")

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
	store := NewMemoryPlanStore()

	_, _, err := store.RecordAudit(context.Background(), AuditRecord{
		PlanID:         "missing-plan",
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-1",
	})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("RecordAudit missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStoreRecordPlanCommandRejectsMissingPlan(t *testing.T) {
	store := NewMemoryPlanStore()

	_, _, err := store.RecordPlanCommand(context.Background(), PlanCommandRecord{
		PlanID:         "missing-plan",
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-1",
	})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("RecordPlanCommand missing plan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestMemoryPlanStoreSavePlanMetricCheckpointRejectsMissingPlan(t *testing.T) {
	store := NewMemoryPlanStore()

	err := store.SavePlanMetricCheckpoint(context.Background(), PlanMetricCheckpoint{
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
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(t, ctx, store, "plan-1")

	err := store.SavePlanMetricCheckpoint(ctx, PlanMetricCheckpoint{
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
	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("plan-1", "start-key")
	status := agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecyclePending}

	first, created, err := store.CreatePlan(context.Background(), spec, status)
	if err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}
	if !created {
		t.Fatal("first CreatePlan was not created")
	}
	second, created, err := store.CreatePlan(context.Background(), spec, agentos.RunPlanStatus{PlanID: "plan-1", LifecycleState: agentos.PlanLifecycleRunning})
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
	store := NewMemoryPlanStore()
	spec := testRunPlanSpec("plan-1", "start-key")
	if _, _, err := store.CreatePlan(context.Background(), spec, agentos.RunPlanStatus{PlanID: "plan-1"}); err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}

	changed := testRunPlanSpec("plan-1", "start-key")
	changed.Nodes[0].NodeID = "changed"
	_, _, err := store.CreatePlan(context.Background(), changed, agentos.RunPlanStatus{PlanID: "plan-1"})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreCreatePlanAllowsReusedKeyAcrossTenantScope(t *testing.T) {
	store := NewMemoryPlanStore()
	if _, _, err := store.CreatePlan(context.Background(), testRunPlanSpec("plan-1", "start-key"), agentos.RunPlanStatus{PlanID: "plan-1"}); err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}

	_, _, err := store.CreatePlan(context.Background(), testRunPlanSpec("plan-2", "start-key"), agentos.RunPlanStatus{PlanID: "plan-2"})
	if err != nil {
		t.Fatalf("CreatePlan different tenant: %v", err)
	}
}

func TestMemoryPlanStoreCreatePlanRejectsReusedKeyWithinTenantScope(t *testing.T) {
	store := NewMemoryPlanStore()
	first := testRunPlanSpec("plan-1", "start-key")
	if _, _, err := store.CreatePlan(context.Background(), first, agentos.RunPlanStatus{PlanID: first.PlanID}); err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}
	second := testRunPlanSpec("plan-2", "start-key")
	second.AccountID = first.AccountID
	second.ProjectID = first.ProjectID

	_, _, err := store.CreatePlan(context.Background(), second, agentos.RunPlanStatus{PlanID: second.PlanID})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreCreatePlanRejectsPlanIDWithDifferentKey(t *testing.T) {
	store := NewMemoryPlanStore()
	if _, _, err := store.CreatePlan(context.Background(), testRunPlanSpec("plan-1", "start-key-1"), agentos.RunPlanStatus{PlanID: "plan-1"}); err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}

	_, _, err := store.CreatePlan(context.Background(), testRunPlanSpec("plan-1", "start-key-2"), agentos.RunPlanStatus{PlanID: "plan-1"})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreGetAuditRecord(t *testing.T) {
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(t, context.Background(), store, "plan-1")
	record := AuditRecord{
		PlanID:         spec.PlanID,
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-1",
	}
	stored, _, err := store.RecordAudit(context.Background(), record)
	if err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}

	got, exists, err := store.GetAuditRecord(context.Background(), AuditRefFromRecord(stored))
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

func TestMemoryPlanStoreRejectsAuditKeyReuseWithDifferentRequest(t *testing.T) {
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(t, context.Background(), store, "plan-1")
	record := AuditRecord{
		PlanID:         spec.PlanID,
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}
	if _, _, err := store.RecordAudit(context.Background(), record); err != nil {
		t.Fatalf("RecordAudit first: %v", err)
	}

	changed := record
	changed.Payload = map[string]any{"type": string(agentos.SignalPlanReject)}
	_, _, err := store.RecordAudit(context.Background(), changed)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreAllowsAuditKeyReuseAcrossPlans(t *testing.T) {
	store := NewMemoryPlanStore()
	first := createMemoryPlanForTest(t, context.Background(), store, "plan-1")
	second := createMemoryPlanForTest(t, context.Background(), store, "plan-2")

	if _, _, err := store.RecordAudit(context.Background(), AuditRecord{
		PlanID:         first.PlanID,
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}); err != nil {
		t.Fatalf("RecordAudit first: %v", err)
	}
	if _, _, err := store.RecordAudit(context.Background(), AuditRecord{
		PlanID:         second.PlanID,
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanReject)},
	}); err != nil {
		t.Fatalf("RecordAudit second: %v", err)
	}
}

func TestMemoryPlanStorePlanCommandLifecycleIsIdempotent(t *testing.T) {
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(t, context.Background(), store, "plan-1")
	command := PlanCommandRecord{
		PlanID:         spec.PlanID,
		ActorID:        "operator-1",
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-1",
		Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
	}
	first, created, err := store.RecordPlanCommand(context.Background(), command)
	if err != nil {
		t.Fatalf("RecordPlanCommand first: %v", err)
	}
	if !created || first.Status != PlanCommandPending {
		t.Fatalf("first command = %#v created=%v", first, created)
	}
	if first.AccountID != spec.AccountID || first.ProjectID != spec.ProjectID {
		t.Fatalf("first command scope = %s/%s, want %s/%s", first.AccountID, first.ProjectID, spec.AccountID, spec.ProjectID)
	}
	replay, created, err := store.RecordPlanCommand(context.Background(), command)
	if err != nil {
		t.Fatalf("RecordPlanCommand replay: %v", err)
	}
	if created || replay.CommandID != first.CommandID {
		t.Fatalf("replay = %#v created=%v, want %#v created=false", replay, created, first)
	}

	delivered, err := store.MarkPlanCommandDelivered(context.Background(), PlanCommandRefFromRecord(first))
	if err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}
	if delivered.Status != PlanCommandDelivered || delivered.FailureReason != "" {
		t.Fatalf("delivered command = %#v", delivered)
	}
}

func TestMemoryPlanStoreListRecoverablePlanCommands(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(t, ctx, store, "plan-1")
	otherSpec := createMemoryPlanForTest(t, ctx, store, "plan-2")
	commands := []PlanCommandRecord{
		{
			CommandID:      "command-delivered",
			PlanID:         spec.PlanID,
			Action:         AuditActionPlanControl,
			IdempotencyKey: "command-delivered",
			Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
			CreatedAt:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
			UpdatedAt:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
		},
		{
			CommandID:      "command-pending",
			PlanID:         spec.PlanID,
			Action:         AuditActionPlanSignal,
			IdempotencyKey: "command-pending",
			Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
			CreatedAt:      time.Date(2026, 6, 19, 12, 1, 0, 0, time.UTC),
			UpdatedAt:      time.Date(2026, 6, 19, 12, 1, 0, 0, time.UTC),
		},
		{
			CommandID:      "command-failed",
			PlanID:         spec.PlanID,
			Action:         AuditActionPlanSignal,
			IdempotencyKey: "command-failed",
			Payload:        map[string]any{"type": string(agentos.SignalPlanReject)},
			CreatedAt:      time.Date(2026, 6, 19, 12, 2, 0, 0, time.UTC),
			UpdatedAt:      time.Date(2026, 6, 19, 12, 2, 0, 0, time.UTC),
		},
		{
			CommandID:      "command-other-tenant",
			PlanID:         otherSpec.PlanID,
			Action:         AuditActionPlanSignal,
			IdempotencyKey: "command-other-tenant",
			Payload:        map[string]any{"type": string(agentos.SignalPlanReject)},
			CreatedAt:      time.Date(2026, 6, 19, 12, 3, 0, 0, time.UTC),
			UpdatedAt:      time.Date(2026, 6, 19, 12, 3, 0, 0, time.UTC),
		},
	}
	for i, command := range commands {
		stored, _, err := store.RecordPlanCommand(ctx, command)
		if err != nil {
			t.Fatalf("RecordPlanCommand %s: %v", command.CommandID, err)
		}
		commands[i] = stored
	}
	if _, err := store.MarkPlanCommandDelivered(ctx, PlanCommandRefFromRecord(commands[0])); err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}
	if _, err := store.MarkPlanCommandFailed(ctx, PlanCommandRefFromRecord(commands[2]), "temporal unavailable"); err != nil {
		t.Fatalf("MarkPlanCommandFailed: %v", err)
	}
	if _, err := store.MarkPlanCommandFailed(ctx, PlanCommandRefFromRecord(commands[3]), "temporal unavailable"); err != nil {
		t.Fatalf("MarkPlanCommandFailed other tenant: %v", err)
	}

	got, err := store.ListRecoverablePlanCommands(ctx, PlanCommandScope{
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

	got, err = store.ListRecoverablePlanCommands(ctx, PlanCommandScope{
		PlanID:   spec.PlanID,
		Statuses: []PlanCommandStatus{PlanCommandFailed},
	})
	if err != nil {
		t.Fatalf("ListRecoverablePlanCommands failed: %v", err)
	}
	if len(got) != 1 || got[0].CommandID != "command-failed" {
		t.Fatalf("failed commands = %#v", got)
	}

	got, err = store.ListRecoverablePlanCommands(ctx, PlanCommandScope{
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Statuses:  []PlanCommandStatus{PlanCommandFailed},
	})
	if err != nil {
		t.Fatalf("ListRecoverablePlanCommands tenant scoped: %v", err)
	}
	if len(got) != 1 || got[0].CommandID != "command-failed" {
		t.Fatalf("tenant scoped failed commands = %#v", got)
	}
}

func TestRecoverablePlanCommandStatusesRejectsDelivered(t *testing.T) {
	_, err := RecoverablePlanCommandStatuses(PlanCommandScope{
		Statuses: []PlanCommandStatus{PlanCommandDelivered},
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestRecoverablePlanCommandStatusesRejectsPartialTenantScope(t *testing.T) {
	for _, scope := range []PlanCommandScope{
		{AccountID: "acct-1"},
		{ProjectID: "proj-1"},
	} {
		_, err := RecoverablePlanCommandStatuses(scope)
		if !errors.Is(err, agentos.ErrInvalidPlanScope) {
			t.Fatalf("RecoverablePlanCommandStatuses(%#v) error = %v, want ErrInvalidPlanScope", scope, err)
		}
	}
}

func TestMemoryPlanStoreRejectsCommandKeyReuseWithDifferentRequest(t *testing.T) {
	store := NewMemoryPlanStore()
	spec := createMemoryPlanForTest(t, context.Background(), store, "plan-1")
	command := PlanCommandRecord{
		PlanID:         spec.PlanID,
		ActorID:        "operator-1",
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}
	if _, _, err := store.RecordPlanCommand(context.Background(), command); err != nil {
		t.Fatalf("RecordPlanCommand first: %v", err)
	}

	changed := command
	changed.Payload = map[string]any{"type": string(agentos.SignalPlanReject)}
	_, _, err := store.RecordPlanCommand(context.Background(), changed)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreAllowsCommandKeyReuseAcrossPlans(t *testing.T) {
	store := NewMemoryPlanStore()
	first := createMemoryPlanForTest(t, context.Background(), store, "plan-1")
	second := createMemoryPlanForTest(t, context.Background(), store, "plan-2")

	if _, _, err := store.RecordPlanCommand(context.Background(), PlanCommandRecord{
		PlanID:         first.PlanID,
		ActorID:        "operator-1",
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
	}); err != nil {
		t.Fatalf("RecordPlanCommand first: %v", err)
	}
	if _, _, err := store.RecordPlanCommand(context.Background(), PlanCommandRecord{
		PlanID:         second.PlanID,
		ActorID:        "operator-1",
		Action:         AuditActionPlanSignal,
		IdempotencyKey: "signal-1",
		Payload:        map[string]any{"type": string(agentos.SignalPlanReject)},
	}); err != nil {
		t.Fatalf("RecordPlanCommand second: %v", err)
	}
}

func TestMemoryPlanStoreListAuditRecordsFiltersAndLimits(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1", IdempotencyKey: "start-1"}
	if _, _, err := store.CreatePlan(ctx, spec, agentos.RunPlanStatus{PlanID: spec.PlanID, LifecycleState: agentos.PlanLifecycleRunning}); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
	records := []AuditRecord{
		{
			AuditID:        "audit-2",
			PlanID:         spec.PlanID,
			NodeID:         "node-1",
			Action:         AuditActionPlanSignal,
			IdempotencyKey: "signal-1",
			CreatedAt:      time.Date(2026, 6, 19, 12, 1, 0, 0, time.UTC),
		},
		{
			AuditID:        "audit-1",
			PlanID:         spec.PlanID,
			NodeID:         "node-1",
			Action:         AuditActionPlanControl,
			IdempotencyKey: "control-1",
			CreatedAt:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
		},
		{
			AuditID:        "audit-other-node",
			PlanID:         spec.PlanID,
			NodeID:         "node-2",
			Action:         AuditActionPlanControl,
			IdempotencyKey: "control-2",
			CreatedAt:      time.Date(2026, 6, 19, 12, 2, 0, 0, time.UTC),
		},
	}
	for _, record := range records {
		if _, _, err := store.RecordAudit(ctx, record); err != nil {
			t.Fatalf("RecordAudit %s: %v", record.AuditID, err)
		}
	}

	got, err := store.ListAuditRecords(ctx, agentos.PlanAuditScope{
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
		if _, err := store.ListAuditRecords(ctx, scope); !errors.Is(err, agentos.ErrInvalidPlanScope) {
			t.Fatalf("ListAuditRecords scope %#v error = %v, want ErrInvalidPlanScope", scope, err)
		}
	}
	if _, err := store.ListAuditRecords(ctx, agentos.PlanAuditScope{PlanID: spec.PlanID, AccountID: "acct-other", ProjectID: spec.ProjectID}); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
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

func createMemoryPlanForTest(t *testing.T, ctx context.Context, store *MemoryPlanStore, planID string) agentos.RunPlanSpec {
	t.Helper()

	spec := testRunPlanSpec(planID, planID+"-start-key")
	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
	}
	if _, _, err := store.CreatePlan(ctx, spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	return spec
}
