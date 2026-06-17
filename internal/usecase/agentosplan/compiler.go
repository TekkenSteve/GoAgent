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
