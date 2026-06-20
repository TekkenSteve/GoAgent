package agentosplan

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// RunPlanCompiler parses serialized plans into validated executable plans.
type RunPlanCompiler struct {
	Validator Validator
}

// CompileJSON compiles JSON into an executable RunPlan.
func (c RunPlanCompiler) CompileJSON(ctx context.Context, data []byte) (ExecutablePlan, error) {
	spec, err := DecodeWireJSON[agentos.RunPlanSpec](data)
	if err != nil {
		return ExecutablePlan{}, err
	}

	return c.Validator.Validate(ctx, spec)
}

// CompileYAML compiles YAML into an executable RunPlan.
func (c RunPlanCompiler) CompileYAML(ctx context.Context, data []byte) (ExecutablePlan, error) {
	spec, err := DecodeWireYAML[agentos.RunPlanSpec](data)
	if err != nil {
		return ExecutablePlan{}, err
	}

	return c.Validator.Validate(ctx, spec)
}

// CompileDeltaJSON decodes JSON PlanDelta and validates the expanded plan.
func (c RunPlanCompiler) CompileDeltaJSON(ctx context.Context, base agentos.RunPlanSpec, data []byte, expansionCount int32) (agentos.RunPlanSpec, ExecutablePlan, error) {
	delta, err := DecodeWireJSON[PlanDelta](data)
	if err != nil {
		return agentos.RunPlanSpec{}, ExecutablePlan{}, fmt.Errorf("decode delta: %w", err)
	}

	return ApplyDelta(ctx, c.Validator, base, delta, expansionCount)
}

// CompileDeltaYAML decodes YAML PlanDelta and validates the expanded plan.
func (c RunPlanCompiler) CompileDeltaYAML(ctx context.Context, base agentos.RunPlanSpec, data []byte, expansionCount int32) (agentos.RunPlanSpec, ExecutablePlan, error) {
	delta, err := DecodeWireYAML[PlanDelta](data)
	if err != nil {
		return agentos.RunPlanSpec{}, ExecutablePlan{}, fmt.Errorf("decode delta: %w", err)
	}

	return ApplyDelta(ctx, c.Validator, base, delta, expansionCount)
}
