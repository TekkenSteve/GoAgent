package agentosplan

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"sigs.k8s.io/yaml"
)

// RunPlanCompiler parses serialized plans into validated executable plans.
type RunPlanCompiler struct {
	Validator Validator
}

// CompileJSON compiles JSON into an executable RunPlan.
func (c RunPlanCompiler) CompileJSON(ctx context.Context, data []byte) (ExecutablePlan, error) {
	var spec agentos.RunPlanSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return ExecutablePlan{}, fmt.Errorf("%w: decode json: %s", agentos.ErrInvalidRunPlan, err)
	}

	return c.Validator.Validate(ctx, spec)
}

// CompileYAML compiles YAML into an executable RunPlan.
func (c RunPlanCompiler) CompileYAML(ctx context.Context, data []byte) (ExecutablePlan, error) {
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return ExecutablePlan{}, fmt.Errorf("%w: decode yaml: %s", agentos.ErrInvalidRunPlan, err)
	}

	return c.CompileJSON(ctx, jsonData)
}

// CompileDeltaJSON decodes JSON PlanDelta and validates the expanded plan.
func (c RunPlanCompiler) CompileDeltaJSON(ctx context.Context, base agentos.RunPlanSpec, data []byte, expansionCount int32) (agentos.RunPlanSpec, ExecutablePlan, error) {
	var delta PlanDelta
	if err := json.Unmarshal(data, &delta); err != nil {
		return agentos.RunPlanSpec{}, ExecutablePlan{}, fmt.Errorf("%w: decode delta json: %s", agentos.ErrInvalidRunPlan, err)
	}

	return ApplyDelta(ctx, c.Validator, base, delta, expansionCount)
}

// CompileDeltaYAML decodes YAML PlanDelta and validates the expanded plan.
func (c RunPlanCompiler) CompileDeltaYAML(ctx context.Context, base agentos.RunPlanSpec, data []byte, expansionCount int32) (agentos.RunPlanSpec, ExecutablePlan, error) {
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return agentos.RunPlanSpec{}, ExecutablePlan{}, fmt.Errorf("%w: decode delta yaml: %s", agentos.ErrInvalidRunPlan, err)
	}

	return c.CompileDeltaJSON(ctx, base, jsonData, expansionCount)
}
