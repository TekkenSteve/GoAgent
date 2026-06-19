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

	err := rt.SignalPlan(t.Context(), "plan-1", agentos.Signal{
		Type:           agentos.SignalUserMessage,
		IdempotencyKey: "message-1",
	})
	if !errors.Is(err, agentos.ErrInvalidSignal) {
		t.Fatalf("SignalPlan error = %v, want ErrInvalidSignal", err)
	}

	err = rt.SignalPlan(t.Context(), "plan-1", agentos.Signal{
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
	got, err := rt.StatusPlan(t.Context(), spec.PlanID)
	if err != nil {
		t.Fatalf("StatusPlan: %v", err)
	}
	if got.PlanID != spec.PlanID || got.LifecycleState != agentos.PlanLifecycleSucceeded {
		t.Fatalf("status = %#v", got)
	}
}

func TestPlanRuntimeStatusPlanReportsMissingDurablePlan(t *testing.T) {
	rt := &planRuntime{planIndex: agentosplan.NewMemoryPlanStore()}

	_, err := rt.StatusPlan(t.Context(), "missing-plan")
	if !errors.Is(err, agentos.ErrPlanRouteNotFound) {
		t.Fatalf("StatusPlan error = %v, want ErrPlanRouteNotFound", err)
	}
}

func TestPlanRuntimeSignalPlanDoesNotAuditFailedDelivery(t *testing.T) {
	store := agentosplan.NewMemoryPlanStore()
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("temporal unavailable")}
	rt := &planRuntime{temporalClient: temporalClient, auditStore: store}

	err := rt.SignalPlan(t.Context(), "plan-1", agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
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
	store := agentosplan.NewMemoryPlanStore()
	if _, _, err := store.RecordAudit(t.Context(), agentosplan.AuditRecord{
		PlanID:         "plan-1",
		Action:         agentosplan.AuditActionPlanSignal,
		IdempotencyKey: "approve-1",
	}); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}
	temporalClient := &fakePlanTemporalClient{signalErr: errors.New("should not signal")}
	rt := &planRuntime{temporalClient: temporalClient, auditStore: store}

	if err := rt.SignalPlan(t.Context(), "plan-1", agentos.Signal{
		Type:           agentos.SignalPlanApprove,
		IdempotencyKey: "approve-1",
	}); err != nil {
		t.Fatalf("SignalPlan: %v", err)
	}
	if temporalClient.signalCount != 0 {
		t.Fatalf("signal count = %d, want 0", temporalClient.signalCount)
	}
}

func TestPlanRuntimeControlPlanAuditsAfterDelivery(t *testing.T) {
	store := agentosplan.NewMemoryPlanStore()
	temporalClient := &fakePlanTemporalClient{}
	rt := &planRuntime{temporalClient: temporalClient, auditStore: store}

	err := rt.ControlPlan(t.Context(), "plan-1", agentos.ControlRequest{
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
