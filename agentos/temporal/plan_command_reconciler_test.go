package temporal

import (
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func TestPlanCommandReconcilerDeliversRecoverableCommands(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)
	signal := agentoscore.Signal{
		Type:           agentoscore.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}

	signalCommand, _, err := recordPlanCommandForTest(t, store, commandFromAudit(planSignalAuditRecord(ref, &signal)))
	if err != nil {
		t.Fatalf("RecordPlanCommand signal: %v", err)
	}

	if _, err := store.MarkPlanCommandFailed(t.Context(), agentosplan.PlanCommandRefFromRecord(&signalCommand), "previous delivery failed"); err != nil {
		t.Fatalf("MarkPlanCommandFailed: %v", err)
	}

	control := agentoscore.ControlRequest{
		Operation:      agentoscore.ControlCancel,
		IdempotencyKey: "cancel-1",
		ActorID:        "operator-1",
		Metadata:       map[string]string{"reason": "operator"},
	}
	if _, _, err := recordPlanCommandForTest(t, store, commandFromAudit(planControlAuditRecord(ref, &control))); err != nil {
		t.Fatalf("RecordPlanCommand control: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{}
	reconciler := newPlanCommandReconciler(temporalClient, testPlanTaskQueues(), store, store, store)

	result, err := reconciler.Recover(t.Context(), 0)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	assertPlanCommandRecoveryResult(t, result, 2, 2, 0)

	if temporalClient.signalCount != 2 {
		t.Fatalf("signal count = %d, want 2", temporalClient.signalCount)
	}

	assertDeliveredPlanCommands(t, store, ref, signal.IdempotencyKey, control.IdempotencyKey)
}

func TestPlanCommandReconcilerMarksInvalidPayloadFailed(t *testing.T) {
	t.Parallel()

	store, ref := newPlanRuntimeTestStore(t)

	invalidCommand := agentosplan.PlanCommandRecord{
		PlanID:         ref.PlanID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		ActorID:        "operator-1",
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: "invalid-signal-1",
		Payload: map[string]any{
			planCommandPayloadSignalType: 42,
		},
	}
	if _, _, err := recordPlanCommandForTest(t, store, &invalidCommand); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{}
	reconciler := newPlanCommandReconciler(temporalClient, testPlanTaskQueues(), store, store, store)

	result, err := reconciler.Recover(t.Context(), 0)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("Recover error = %v, want ErrInvalidRunPlan", err)
	}

	assertPlanCommandRecoveryResult(t, result, 1, 0, 1)

	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}

	command, exists, lookupErr := store.GetPlanCommand(t.Context(), planCommandRef(ref, "invalid-signal-1"))
	if lookupErr != nil || !exists {
		t.Fatalf("command exists=%v err=%v", exists, lookupErr)
	}

	if command.Status != agentosplan.PlanCommandFailed || command.FailureReason == "" {
		t.Fatalf("command = %#v, want failed with reason", command)
	}
}

func TestCommandDeliveryPayloadPreservesCommandTimestamps(t *testing.T) {
	t.Parallel()

	ref := agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}
	sentAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	signal := agentoscore.Signal{
		Type:           agentoscore.SignalPlanReject,
		IdempotencyKey: "reject-1",
		ActorID:        "operator-1",
		Payload:        map[string]any{agentoscore.SignalPayloadReason: "not ready"},
		SentAt:         sentAt,
	}
	signalCommand := commandFromAudit(planSignalAuditRecord(ref, &signal))

	assertCommandDeliveryTimestamp(t, signalCommand, &commandTimestampAssertion{
		label:       "signal",
		signalName:  PlanSignalName,
		auditKey:    planCommandPayloadSentAt,
		wantTime:    sentAt,
		payloadTime: signalPayloadSentAt,
	})

	requestedAt := time.Date(2026, 6, 20, 12, 5, 0, 0, time.UTC)
	control := agentoscore.ControlRequest{
		Operation:      agentoscore.ControlPause,
		IdempotencyKey: "pause-1",
		RequestedAt:    requestedAt,
		ActorID:        "operator-1",
		Metadata:       map[string]string{"reason": "maintenance"},
	}
	controlCommand := commandFromAudit(planControlAuditRecord(ref, &control))

	assertCommandDeliveryTimestamp(t, controlCommand, &commandTimestampAssertion{
		label:       "control",
		signalName:  PlanControlSignalName,
		auditKey:    planCommandPayloadRequestedAt,
		wantTime:    requestedAt,
		payloadTime: controlPayloadRequestedAt,
	})
}

type commandTimestampAssertion struct {
	label       string
	signalName  string
	auditKey    string
	wantTime    time.Time
	payloadTime func(any) (time.Time, bool)
}

func assertCommandDeliveryTimestamp(t *testing.T, command *agentosplan.PlanCommandRecord, assertion *commandTimestampAssertion) {
	t.Helper()

	signalName, payload, audit, err := commandDeliveryPayload(command)
	if err != nil {
		t.Fatalf("commandDeliveryPayload %s: %v", assertion.label, err)
	}

	deliveredAt, ok := assertion.payloadTime(payload)
	if !ok {
		t.Fatalf("%s payload type = %T", assertion.label, payload)
	}

	auditAt, ok := audit.Payload[assertion.auditKey].(time.Time)
	if !ok {
		t.Fatalf("%s audit timestamp is not a time.Time", assertion.label)
	}

	if signalName != assertion.signalName || !deliveredAt.Equal(assertion.wantTime) || !auditAt.Equal(assertion.wantTime) {
		t.Fatalf("%s delivery name=%q payload=%#v audit=%#v", assertion.label, signalName, payload, audit)
	}
}

func signalPayloadSentAt(payload any) (time.Time, bool) {
	signal, ok := payload.(agentoscore.Signal)

	return signal.SentAt, ok
}

func controlPayloadRequestedAt(payload any) (time.Time, bool) {
	control, ok := payload.(agentoscore.ControlRequest)

	return control.RequestedAt, ok
}

func TestWorkerKitRecoverPlanCommandsUsesReconciler(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)

	signal := agentoscore.Signal{
		Type:           agentoscore.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}
	if _, _, err := recordPlanCommandForTest(t, store, commandFromAudit(planSignalAuditRecord(ref, &signal))); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	kit := &WorkerKit{planCommandReconciler: newPlanCommandReconciler(&fakePlanTemporalClient{}, testPlanTaskQueues(), store, store, store)}

	result, err := kit.RecoverPlanCommands(t.Context(), 1)
	if err != nil {
		t.Fatalf("RecoverPlanCommands: %v", err)
	}

	assertPlanCommandRecoveryResult(t, result, 1, 1, 0)
}

func TestPlanRuntimeRecoverPlanCommandsUsesReconciler(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)

	signal := agentoscore.Signal{
		Type:           agentoscore.SignalPlanApprove,
		IdempotencyKey: "approve-runtime-1",
		ActorID:        "operator-1",
	}
	if _, _, err := recordPlanCommandForTest(t, store, commandFromAudit(planSignalAuditRecord(ref, &signal))); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{}
	taskQueues := testPlanTaskQueues()
	rt := &planRuntime{
		temporalClient: temporalClient,
		taskQueue:      taskQueues.PlanControl,
		taskQueues:     taskQueues,
		commandStore:   store,
		auditStore:     store,
		planIndex:      store,
	}

	result, err := rt.RecoverPlanCommands(t.Context(), 1)
	if err != nil {
		t.Fatalf("RecoverPlanCommands: %v", err)
	}

	assertPlanCommandRecoveryResult(t, result, 1, 1, 0)

	if temporalClient.signalCount != 1 {
		t.Fatalf("signal count = %d, want 1", temporalClient.signalCount)
	}
}

func TestPlanCommandReconcilerDeliversPlanStart(t *testing.T) {
	t.Parallel()
	store, ref := newPlanRuntimeTestStore(t)

	spec, _, exists, err := store.GetPlanByRef(t.Context(), ref)
	if err != nil || !exists {
		t.Fatalf("GetPlanByRef exists=%v err=%v", exists, err)
	}

	if _, _, err := recordPlanCommandForTest(t, store, commandFromAudit(planStartAuditRecord(&spec))); err != nil {
		t.Fatalf("RecordPlanCommand: %v", err)
	}

	temporalClient := &fakePlanTemporalClient{}
	taskQueues := testPlanTaskQueues()
	reconciler := newPlanCommandReconciler(temporalClient, taskQueues, store, store, store)

	result, err := reconciler.Recover(t.Context(), 1)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	assertPlanCommandRecoveryResult(t, result, 1, 1, 0)

	if temporalClient.executeCount != 1 || temporalClient.executeTaskQueue != taskQueues.PlanControl {
		t.Fatalf("execute count=%d taskQueue=%q", temporalClient.executeCount, temporalClient.executeTaskQueue)
	}

	assertDeliveredPlanCommands(t, store, ref, spec.IdempotencyKey)
}

func testPlanTaskQueues() *TaskQueues {
	taskQueues := DefaultTaskQueues()

	return &taskQueues
}

func commandFromAudit(record *agentosplan.AuditRecord) *agentosplan.PlanCommandRecord {
	command := planCommandFromAuditRecord(record)

	return &command
}

func recordPlanCommandForTest(
	t *testing.T,
	store agentosplan.PlanCommandStore,
	command *agentosplan.PlanCommandRecord,
) (agentosplan.PlanCommandRecord, bool, error) {
	t.Helper()

	return store.RecordPlanCommand(t.Context(), command)
}

func assertPlanCommandRecoveryResult(t *testing.T, result PlanCommandRecoveryResult, scanned, delivered, failed int) {
	t.Helper()

	if result.Scanned != scanned || result.Delivered != delivered || result.Failed != failed {
		t.Fatalf("result = %#v, want scanned=%d delivered=%d failed=%d", result, scanned, delivered, failed)
	}
}

func assertDeliveredPlanCommands(t *testing.T, store interface {
	agentosplan.PlanCommandStore
	agentosplan.AuditStore
}, ref agentos.PlanRef, keys ...string,
) {
	t.Helper()

	for _, key := range keys {
		command, exists, err := store.GetPlanCommand(t.Context(), planCommandRef(ref, key))
		if err != nil || !exists {
			t.Fatalf("command %s exists=%v err=%v", key, exists, err)
		}

		if command.Status != agentosplan.PlanCommandDelivered {
			t.Fatalf("command %s = %#v, want delivered", key, command)
		}

		_, exists, err = store.GetAuditRecord(t.Context(), planAuditRef(ref, key))
		if err != nil || !exists {
			t.Fatalf("audit %s exists=%v err=%v", key, exists, err)
		}
	}
}
