package agentosplan

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// PlanDelta is the only supported dynamic expansion unit. It is intended to be
// produced and validated inside PlanWorkflow, not patched arbitrarily by clients.
type PlanDelta struct {
	Nodes []agentos.PlanNodeSpec `json:"nodes,omitempty"`
	Edges []agentos.PlanEdgeSpec `json:"edges,omitempty"`
}

// ApplyDelta validates and appends nodes/edges under the plan policy limits.
func ApplyDelta(ctx context.Context, validator Validator, current agentos.RunPlanSpec, delta PlanDelta, expansionCount int32) (agentos.RunPlanSpec, ExecutablePlan, error) {
	policy := normalizePolicy(current.Policy)
	if expansionCount >= policy.MaxExpansions {
		return agentos.RunPlanSpec{}, ExecutablePlan{}, fmt.Errorf("%w: expansion count exceeds max %d", agentos.ErrInvalidRunPlan, policy.MaxExpansions)
	}
	if int32(len(current.Nodes)+len(delta.Nodes)) > policy.MaxNodes {
		return agentos.RunPlanSpec{}, ExecutablePlan{}, fmt.Errorf("%w: delta exceeds max nodes %d", agentos.ErrInvalidRunPlan, policy.MaxNodes)
	}

	next := current
	next.Nodes = append(append([]agentos.PlanNodeSpec{}, current.Nodes...), delta.Nodes...)
	next.Edges = append(append([]agentos.PlanEdgeSpec{}, current.Edges...), delta.Edges...)

	plan, err := validator.Validate(ctx, next)
	if err != nil {
		return agentos.RunPlanSpec{}, ExecutablePlan{}, err
	}

	return next, plan, nil
}
