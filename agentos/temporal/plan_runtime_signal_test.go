package temporal

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
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
