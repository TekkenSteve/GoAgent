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
	_, _, err := store.CreatePlan(context.Background(), agentos.RunPlanSpec{PlanID: "plan-1"}, agentos.RunPlanStatus{})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestMemoryPlanStoreSavePlanStateRequiresIdempotencyKey(t *testing.T) {
	store := NewMemoryPlanStore()
	err := store.SavePlanState(context.Background(), PlanStateSnapshot{
		Spec:   agentos.RunPlanSpec{PlanID: "plan-1"},
		Status: agentos.RunPlanStatus{PlanID: "plan-1"},
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
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

func TestMemoryPlanStoreCreatePlanRejectsReusedKeyForDifferentPlan(t *testing.T) {
	store := NewMemoryPlanStore()
	if _, _, err := store.CreatePlan(context.Background(), testRunPlanSpec("plan-1", "start-key"), agentos.RunPlanStatus{PlanID: "plan-1"}); err != nil {
		t.Fatalf("CreatePlan first: %v", err)
	}

	_, _, err := store.CreatePlan(context.Background(), testRunPlanSpec("plan-2", "start-key"), agentos.RunPlanStatus{PlanID: "plan-2"})
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
	record := AuditRecord{
		PlanID:         "plan-1",
		Action:         AuditActionPlanControl,
		IdempotencyKey: "control-1",
	}
	if _, _, err := store.RecordAudit(context.Background(), record); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}

	got, exists, err := store.GetAuditRecord(context.Background(), "control-1")
	if err != nil {
		t.Fatalf("GetAuditRecord: %v", err)
	}
	if !exists || got.PlanID != record.PlanID || got.Action != record.Action {
		t.Fatalf("audit = %#v exists=%v", got, exists)
	}
}

func TestMemoryPlanStoreRejectsAuditKeyReuseWithDifferentRequest(t *testing.T) {
	store := NewMemoryPlanStore()
	record := AuditRecord{
		PlanID:         "plan-1",
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

func TestMemoryPlanStorePlanCommandLifecycleIsIdempotent(t *testing.T) {
	store := NewMemoryPlanStore()
	command := PlanCommandRecord{
		PlanID:         "plan-1",
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
	replay, created, err := store.RecordPlanCommand(context.Background(), command)
	if err != nil {
		t.Fatalf("RecordPlanCommand replay: %v", err)
	}
	if created || replay.CommandID != first.CommandID {
		t.Fatalf("replay = %#v created=%v, want %#v created=false", replay, created, first)
	}

	delivered, err := store.MarkPlanCommandDelivered(context.Background(), command.IdempotencyKey)
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
	commands := []PlanCommandRecord{
		{
			CommandID:      "command-delivered",
			PlanID:         "plan-1",
			Action:         AuditActionPlanControl,
			IdempotencyKey: "command-delivered",
			Payload:        map[string]any{"operation": string(agentos.ControlCancel)},
			CreatedAt:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
			UpdatedAt:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
		},
		{
			CommandID:      "command-pending",
			PlanID:         "plan-1",
			Action:         AuditActionPlanSignal,
			IdempotencyKey: "command-pending",
			Payload:        map[string]any{"type": string(agentos.SignalPlanApprove)},
			CreatedAt:      time.Date(2026, 6, 19, 12, 1, 0, 0, time.UTC),
			UpdatedAt:      time.Date(2026, 6, 19, 12, 1, 0, 0, time.UTC),
		},
		{
			CommandID:      "command-failed",
			PlanID:         "plan-1",
			Action:         AuditActionPlanSignal,
			IdempotencyKey: "command-failed",
			Payload:        map[string]any{"type": string(agentos.SignalPlanReject)},
			CreatedAt:      time.Date(2026, 6, 19, 12, 2, 0, 0, time.UTC),
			UpdatedAt:      time.Date(2026, 6, 19, 12, 2, 0, 0, time.UTC),
		},
	}
	for _, command := range commands {
		if _, _, err := store.RecordPlanCommand(ctx, command); err != nil {
			t.Fatalf("RecordPlanCommand %s: %v", command.CommandID, err)
		}
	}
	if _, err := store.MarkPlanCommandDelivered(ctx, "command-delivered"); err != nil {
		t.Fatalf("MarkPlanCommandDelivered: %v", err)
	}
	if _, err := store.MarkPlanCommandFailed(ctx, "command-failed", "temporal unavailable"); err != nil {
		t.Fatalf("MarkPlanCommandFailed: %v", err)
	}

	got, err := store.ListRecoverablePlanCommands(ctx, PlanCommandScope{
		PlanID:   "plan-1",
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
		PlanID:   "plan-1",
		Statuses: []PlanCommandStatus{PlanCommandFailed},
	})
	if err != nil {
		t.Fatalf("ListRecoverablePlanCommands failed: %v", err)
	}
	if len(got) != 1 || got[0].CommandID != "command-failed" {
		t.Fatalf("failed commands = %#v", got)
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

func TestMemoryPlanStoreRejectsCommandKeyReuseWithDifferentRequest(t *testing.T) {
	store := NewMemoryPlanStore()
	command := PlanCommandRecord{
		PlanID:         "plan-1",
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
	if _, err := store.ListAuditRecords(ctx, agentos.PlanAuditScope{PlanID: spec.PlanID, AccountID: "acct-other", ProjectID: spec.ProjectID}); !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("tenant mismatch error = %v, want ErrPlanRouteNotFound", err)
	}
}

func testRunPlanSpec(planID, idempotencyKey string) agentos.RunPlanSpec {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}

	return agentos.RunPlanSpec{
		PlanID:         planID,
		IdempotencyKey: idempotencyKey,
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "node-1", Run: agentos.RunSpec{RunID: "run-1", Backend: ref}},
		},
	}
}
