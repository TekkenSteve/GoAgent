package temporal

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func TestPlanCommandReconcilerDeliversRecoverableCommands(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	signal := agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}
	if _, _, err := store.RecordPlanCommand(t.Context(), planCommandFromAuditRecord(planSignalAuditRecord(ref.PlanID, signal))); err != nil {
		t.Fatalf("RecordPlanCommand signal: %v", err)
	}
	if _, err := store.MarkPlanCommandFailed(t.Context(), signal.IdempotencyKey, "previous delivery failed"); err != nil {
		t.Fatalf("MarkPlanCommandFailed: %v", err)
	}
	control := agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: "cancel-1",
		ActorID:        "operator-1",
		Metadata:       map[string]string{"reason": "operator"},
	}
	if _, _, err := store.RecordPlanCommand(t.Context(), planCommandFromAuditRecord(planControlAuditRecord(ref.PlanID, control))); err != nil {
		t.Fatalf("RecordPlanCommand control: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{}
	reconciler := newPlanCommandReconciler(temporalClient, store, store)
	result, err := reconciler.Recover(t.Context(), 0)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if result.Scanned != 2 || result.Delivered != 2 || result.Failed != 0 {
		t.Fatalf("result = %#v", result)
	}
	if temporalClient.signalCount != 2 {
		t.Fatalf("signal count = %d, want 2", temporalClient.signalCount)
	}
	for _, key := range []string{signal.IdempotencyKey, control.IdempotencyKey} {
		command, exists, err := store.GetPlanCommand(t.Context(), key)
		if err != nil || !exists {
			t.Fatalf("command %s exists=%v err=%v", key, exists, err)
		}
		if command.Status != agentosplan.PlanCommandDelivered {
			t.Fatalf("command %s = %#v, want delivered", key, command)
		}
		if _, exists, err := store.GetAuditRecord(t.Context(), key); err != nil || !exists {
			t.Fatalf("audit %s exists=%v err=%v", key, exists, err)
		}
	}
}

func TestPlanCommandReconcilerMarksInvalidPayloadFailed(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	if _, _, err := store.RecordPlanCommand(t.Context(), agentosplan.PlanCommandRecord{
		PlanID:         ref.PlanID,
		ActorID:        "operator-1",
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: "invalid-signal-1",
		Payload: map[string]any{
			planCommandPayloadSignalType: 42,
		},
	}); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{}
	reconciler := newPlanCommandReconciler(temporalClient, store, store)
	result, err := reconciler.Recover(t.Context(), 0)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("Recover error = %v, want ErrInvalidRunPlan", err)
	}
	if result.Scanned != 1 || result.Delivered != 0 || result.Failed != 1 {
		t.Fatalf("result = %#v", result)
	}
	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}
	command, exists, lookupErr := store.GetPlanCommand(t.Context(), "invalid-signal-1")
	if lookupErr != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, lookupErr)
	}
	if command.Status != agentosplan.PlanCommandFailed || command.FailureReason == "" {
		t.Fatalf("command = %#v, want failed with reason", command)
	}
}

func TestWorkerKitRecoverPlanCommandsUsesReconciler(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	signal := agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}
	if _, _, err := store.RecordPlanCommand(t.Context(), planCommandFromAuditRecord(planSignalAuditRecord(ref.PlanID, signal))); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	kit := &WorkerKit{planCommandReconciler: newPlanCommandReconciler(&fakePlanTemporalClient{}, store, store)}
	result, err := kit.RecoverPlanCommands(t.Context(), 1)
	if err != nil {
		t.Fatalf("RecoverPlanCommands: %v", err)
	}
	if result.Scanned != 1 || result.Delivered != 1 || result.Failed != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestPlanRuntimeRecoverPlanCommandsUsesReconciler(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	signal := agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-runtime-1",
		ActorID:        "operator-1",
	}
	if _, _, err := store.RecordPlanCommand(t.Context(), planCommandFromAuditRecord(planSignalAuditRecord(ref.PlanID, signal))); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{}
	rt := &planRuntime{temporalClient: temporalClient, commandStore: store, auditStore: store}
	result, err := rt.RecoverPlanCommands(t.Context(), 1)
	if err != nil {
		t.Fatalf("RecoverPlanCommands: %v", err)
	}
	if result.Scanned != 1 || result.Delivered != 1 || result.Failed != 0 {
		t.Fatalf("result = %#v", result)
	}
	if temporalClient.signalCount != 1 {
		t.Fatalf("signal count = %d, want 1", temporalClient.signalCount)
	}
}
