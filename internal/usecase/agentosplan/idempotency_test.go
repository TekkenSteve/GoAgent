package agentosplan

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestValidatePlanStartIdempotencyRejectsDifferentPlanID(t *testing.T) {
	err := ValidatePlanStartIdempotency(
		agentos.RunPlanSpec{PlanID: "plan-1", IdempotencyKey: "start-key"},
		agentos.RunPlanSpec{PlanID: "plan-2", IdempotencyKey: "start-key"},
	)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatePlanStartIdempotencyRejectsDifferentRequest(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	existing := agentos.RunPlanSpec{
		PlanID:         "plan-1",
		IdempotencyKey: "start-key",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "node-1", Run: agentos.RunSpec{RunID: "run-1", Backend: ref}},
		},
	}
	requested := existing
	requested.Nodes = []agentos.PlanNodeSpec{
		{NodeID: "node-2", Run: agentos.RunSpec{RunID: "run-2", Backend: ref}},
	}

	err := ValidatePlanStartIdempotency(existing, requested)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}
