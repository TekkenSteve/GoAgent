package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"go.temporal.io/sdk/client"
)

func TestPlanRuntimeSignalPlanValidatesSignalBeforeAudit(t *testing.T) {
	rt := &planRuntime{}
	ref := agentos.PlanRef{PlanID: "plan-1", AccountID: "acct-1"}

	err := rt.SignalPlan(t.Context(), ref, agentos.Signal{
		Type:           agentos.SignalUserMessage,
		IdempotencyKey: "message-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("SignalPlan error = %v, want ErrInvalidSignal", err)
	}

	err = rt.SignalPlan(t.Context(), ref, agentos.Signal{
		Type:           agentos.SignalPlanNodeRetry,
		IdempotencyKey: "retry-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("SignalPlan retry error = %v, want ErrInvalidSignal", err)
	}
}

func TestPlanRuntimeStatusPlanReadsDurableIndex(t *testing.T) {
	store := agentosplan.NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-1",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "node-1",
				Run: agentos.RunSpec{
					RunID:   "run-1",
					Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "http-agent"},
				},
			},
		},
	}
	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleSucceeded,
		UpdatedAt:      time.Now().UTC(),
	}
	if _, _, err := store.CreatePlan(t.Context(), spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	rt := &planRuntime{planIndex: store}
	got, err := rt.StatusPlan(t.Context(), agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID})
	if err != nil {
		t.Fatalf("StatusPlan: %v", err)
	}
	if got.PlanID != spec.PlanID || got.LifecycleState != agentos.PlanLifecycleSucceeded {
		t.Fatalf("status = %#v", got)
	}
}

func TestPlanRuntimeStartPlanRequiresAccountScope(t *testing.T) {
	rt := &planRuntime{planIndex: agentosplan.NewMemoryPlanStore()}

	_, err := rt.StartPlan(t.Context(), agentos.RunPlanSpec{
		PlanID:         "plan-1",
		IdempotencyKey: "plan-start-1",
	})
	if !errors.Is(err, agentos.ErrInvalidPlanScope) {
		t.Fatalf("StartPlan error = %v, want ErrInvalidPlanScope", err)
	}
}

func TestPlanRuntimeWithoutPostgresDoesNotInstallMemoryPlanStores(t *testing.T) {
	rt, err := newPlanRuntimeWithClient(t.Context(), RuntimeConfig{TemporalTaskQueue: "agentos-test"}, &fakePlanTemporalClient{}, false)
	if err != nil {
		t.Fatalf("newPlanRuntimeWithClient: %v", err)
	}
	if rt.planIndex != nil || rt.planEvents != nil || rt.auditStore != nil {
		t.Fatalf("durable stores = index:%T events:%T audit:%T, want nil without postgres", rt.planIndex, rt.planEvents, rt.auditStore)
	}

	_, err = rt.StartPlan(t.Context(), agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		IdempotencyKey: "plan-start-1",
	})
	if !errors.Is(err, errPlanRuntimePlanIndexRequired) {
		t.Fatalf("StartPlan error = %v, want missing durable plan index", err)
	}
	_, err = rt.SubscribePlan(t.Context(), agentos.PlanStreamScope{PlanID: "plan-1", AccountID: "acct-1"})
	if !errors.Is(err, errPlanRuntimePlanEventStoreRequired) {
		t.Fatalf("SubscribePlan error = %v, want missing durable plan event store", err)
	}
}

func TestPlanRuntimeStatusPlanReportsMissingDurablePlan(t *testing.T) {
	rt := &planRuntime{planIndex: agentosplan.NewMemoryPlanStore()}

	_, err := rt.StatusPlan(t.Context(), agentos.PlanRef{PlanID: "missing-plan", AccountID: "acct-1"})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("StatusPlan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestPlanRuntimeStatusPlanRejectsTenantMismatch(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	rt := &planRuntime{planIndex: store}

	ref.AccountID = "acct-other"
	_, err := rt.StatusPlan(t.Context(), ref)
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("StatusPlan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestPlanRuntimeSubscribePlanEnforcesTenantScope(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	if _, err := store.AppendPlanEvent(t.Context(), agentos.PlanEvent{Event: agentos.Event{EventType: agentos.EventPlanStarted}, PlanID: ref.PlanID}, "event-1"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}
	rt := &planRuntime{planIndex: store, planEvents: store}

	_, err := rt.SubscribePlan(t.Context(), agentos.PlanStreamScope{PlanID: ref.PlanID, AccountID: "acct-other", ProjectID: ref.ProjectID})
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("SubscribePlan mismatch error = %v, want ErrPlanRouteNotFound", err)
	}

	sub, err := rt.SubscribePlan(t.Context(), agentos.PlanStreamScope{PlanID: ref.PlanID, AccountID: ref.AccountID, ProjectID: ref.ProjectID})
	if err != nil {
		t.Fatalf("SubscribePlan scoped: %v", err)
	}
	defer sub.Close()
	event := <-sub.Events()
	if event.EventType != agentos.EventPlanStarted {
		t.Fatalf("event = %#v", event)
	}
}

func TestPlanRuntimeSignalPlanDoesNotAuditFailedDelivery(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("temporal unavailable")}
	rt := &planRuntime{temporalClient: temporalClient, auditStore: store, planIndex: store}

	err := rt.SignalPlan(t.Context(), ref, agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	})
	if err == nil {
		t.Fatal("SignalPlan succeeded, want delivery error")
	}
	if temporalClient.signalCount != 1 {
		t.Fatalf("signal count = %d, want 1", temporalClient.signalCount)
	}
	if _, exists, lookupErr := store.GetAuditRecord(t.Context(), "approve-1"); lookupErr != nil || exists {
		t.Fatalf("audit exists=%v err=%v, want no audit", exists, lookupErr)
	}
}

func TestPlanRuntimeSignalPlanSkipsDeliveryWhenAuditExists(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	signal := agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
	}
	if _, _, err := store.RecordAudit(t.Context(), planSignalAuditRecord("plan-1", signal)); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("should not signal")}
	rt := &planRuntime{temporalClient: temporalClient, auditStore: store, planIndex: store}

	if err := rt.SignalPlan(t.Context(), ref, signal); err != nil {
		t.Fatalf("SignalPlan: %v", err)
	}
	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}
}

func TestPlanRuntimeSignalPlanRejectsAuditKeyReuseWithDifferentSignal(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	if _, _, err := store.RecordAudit(t.Context(), planSignalAuditRecord("plan-1", agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "signal-1",
		ActorID:        "operator-1",
	})); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("should not signal")}
	rt := &planRuntime{temporalClient: temporalClient, auditStore: store, planIndex: store}

	err := rt.SignalPlan(t.Context(), ref, agentos.Signal{
		Type:           agentos.SignalPlanReject,
		IdempotencyKey: "signal-1",
		ActorID:        "operator-1",
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("SignalPlan error = %v, want ErrInvalidRunPlan", err)
	}
	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}
}

func TestPlanRuntimeControlPlanAuditsAfterDelivery(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	temporalClient := &fakePlanTemporalClient{}
	rt := &planRuntime{temporalClient: temporalClient, auditStore: store, planIndex: store}

	err := rt.ControlPlan(t.Context(), ref, agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: "cancel-1",
		ActorID:        "operator-1",
	})
	if err != nil {
		t.Fatalf("ControlPlan: %v", err)
	}
	if temporalClient.signalName != PlanControlSignalName || temporalClient.signalCount != 1 {
		t.Fatalf("signal name=%q count=%d", temporalClient.signalName, temporalClient.signalCount)
	}
	record, exists, err := store.GetAuditRecord(t.Context(), "cancel-1")
	if err != nil || !exists {
		t.Fatalf("audit exists=%v err=%v", exists, err)
	}
	if record.ActorID != "operator-1" || record.Action != agentosplan.AuditActionPlanControl {
		t.Fatalf("audit = %#v", record)
	}
}

func TestPlanRuntimeControlPlanRejectsAuditKeyReuseWithDifferentControl(t *testing.T) {
	store, ref := newPlanRuntimeTestStore(t)
	if _, _, err := store.RecordAudit(t.Context(), agentosplan.AuditRecord{
		PlanID:         "plan-1",
		ActorID:        "operator-1",
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: "control-1",
		Payload: map[string]any{
			"operation": agentos.ControlCancel,
			"metadata":  map[string]string(nil),
		},
	}); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("should not signal")}
	rt := &planRuntime{temporalClient: temporalClient, auditStore: store, planIndex: store}

	err := rt.ControlPlan(t.Context(), ref, agentos.ControlRequest{
		Operation:      agentos.ControlPause,
		IdempotencyKey: "control-1",
		ActorID:        "operator-1",
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("ControlPlan error = %v, want ErrInvalidRunPlan", err)
	}
	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}
}

func newPlanRuntimeTestStore(t *testing.T) (*agentosplan.MemoryPlanStore, agentos.PlanRef) {
	t.Helper()

	store := agentosplan.NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-1",
	}
	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      time.Now().UTC(),
	}
	if _, _, err := store.CreatePlan(t.Context(), spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	return store, agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}
}

type fakePlanTemporalClient struct {
	signalName  string
	signalCount int
	signalErr   error
}

func (c *fakePlanTemporalClient) ExecuteWorkflow(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error) {
	return nil, nil
}

func (c *fakePlanTemporalClient) SignalWorkflow(_ context.Context, _ string, _ string, signalName string, _ interface{}) error {
	c.signalName = signalName
	c.signalCount++

	return c.signalErr
}

func (c *fakePlanTemporalClient) Close() {}
