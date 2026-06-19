package agentosplan

import (
	"context"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestMemoryPlanStoreCreatePlanRequiresIdempotencyKey(t *testing.T) {
	store := NewMemoryPlanStore()
	_, _, err := store.CreatePlan(context.Background(), agentos.RunPlanSpec{PlanID: "plan-1"}, agentos.RunPlanStatus{})
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
