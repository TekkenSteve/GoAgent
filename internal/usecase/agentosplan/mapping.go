package agentosplan

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ResolveRunInput applies plan/node input mappings and dereferences artifact refs
// before a backend-owned child run starts.
func ResolveRunInput(ctx context.Context, store ArtifactStore, expressions ValueExpressionCompiler, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, node *agentos.PlanNodeSpec, edges []agentos.PlanEdgeSpec) (map[string]any, error) {
	resolved := cloneMap(node.Run.Input)
	mappings := runInputMappings(node, edges)

	if len(mappings) == 0 {
		return resolved, nil
	}

	data, err := json.Marshal(resolved)
	if err != nil {
		return nil, fmt.Errorf("%w: encode node input: %w", agentos.ErrInvalidRunPlan, err)
	}

	for i := range mappings {
		mapping := &mappings[i]
		if _, err := validateInputMappingShape(fmt.Sprintf("node %q", node.NodeID), mapping); err != nil {
			return nil, err
		}

		value, err := resolveMappingValue(ctx, store, expressions, spec, status, node, mapping)
		if err != nil {
			return nil, err
		}

		if value == nil && !mapping.Required {
			continue
		}

		data, err = sjson.SetBytes(data, mapping.Target, value)
		if err != nil {
			return nil, fmt.Errorf("%w: set input mapping %q: %w", agentos.ErrInvalidRunPlan, mapping.Target, err)
		}
	}

	var output map[string]any
	if err := json.Unmarshal(data, &output); err != nil {
		return nil, fmt.Errorf("%w: decode mapped input: %w", agentos.ErrInvalidRunPlan, err)
	}

	return output, nil
}

func runInputMappings(node *agentos.PlanNodeSpec, edges []agentos.PlanEdgeSpec) []agentos.InputMapping {
	mappings := make([]agentos.InputMapping, 0, len(node.Inputs)+len(edges))
	mappings = append(mappings, node.Inputs...)

	for i := range edges {
		mappings = append(mappings, edges[i].InputMapping...)
	}

	return mappings
}

func resolveMappingValue(ctx context.Context, store ArtifactStore, expressions ValueExpressionCompiler, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, node *agentos.PlanNodeSpec, mapping *agentos.InputMapping) (any, error) {
	if mapping.Expression != "" {
		if expressions == nil {
			return nil, fmt.Errorf("%w: expression compiler is required for mapping %q", agentos.ErrInvalidExpression, mapping.Target)
		}

		compiled, err := expressions.CompileValue(mapping.Expression)
		if err != nil {
			return nil, err
		}

		return compiled.EvaluateValue(ctx, mappingVariables(spec, status, node))
	}

	if mapping.SourceArtifact != "" {
		return resolveArtifactMapping(ctx, store, spec, status, mapping)
	}

	if mapping.SourcePath != "" {
		return selectSourcePath(spec.Inputs, mapping.SourcePath, mapping.Required)
	}

	return nil, nil
}

func resolveArtifactMapping(ctx context.Context, store ArtifactStore, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, mapping *agentos.InputMapping) (any, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: artifact store is required for mapping %q", agentos.ErrInvalidArtifact, mapping.Target)
	}

	ref, ok := findArtifact(status.Artifacts, mapping.SourceNodeID, mapping.SourceArtifact)
	if !ok {
		if mapping.Required {
			return nil, fmt.Errorf("%w: required artifact %q is missing", agentos.ErrArtifactNotFound, mapping.SourceArtifact)
		}

		return nil, nil
	}

	if ref.PlanID != "" && ref.PlanID != spec.PlanID {
		return nil, fmt.Errorf("%w: artifact %q belongs to plan %q", agentos.ErrInvalidArtifact, mapping.SourceArtifact, ref.PlanID)
	}

	if ref.ArtifactID == "" {
		return nil, fmt.Errorf("%w: artifact %q has no artifact id", agentos.ErrInvalidArtifact, mapping.SourceArtifact)
	}

	scope := agentos.PlanArtifactScope{
		PlanID:     spec.PlanID,
		AccountID:  spec.AccountID,
		ProjectID:  spec.ProjectID,
		ArtifactID: ref.ArtifactID,
	}

	_, payload, err := store.Get(ctx, &scope)
	if err != nil {
		return nil, err
	}

	if payload == nil {
		return nil, fmt.Errorf("%w: artifact %q has no payload", agentos.ErrArtifactNotFound, mapping.SourceArtifact)
	}

	return selectSourcePath(payload, mapping.SourcePath, mapping.Required)
}

func mappingVariables(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, node *agentos.PlanNodeSpec) map[string]any {
	vars := Variables(spec, status)
	vars["node"] = *node

	return vars
}

func selectSourcePath(source any, sourcePath string, required bool) (any, error) {
	if sourcePath == "" {
		return source, nil
	}

	data, err := json.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("%w: encode mapping source: %w", agentos.ErrInvalidRunPlan, err)
	}

	result := gjson.GetBytes(data, sourcePath)
	if !result.Exists() {
		if required {
			return nil, fmt.Errorf("%w: required source path %q is missing", agentos.ErrInvalidRunPlan, sourcePath)
		}

		return nil, nil
	}

	return result.Value(), nil
}

func findArtifact(refs []agentos.ArtifactRef, nodeID, name string) (agentos.ArtifactRef, bool) {
	for i := range refs {
		ref := refs[i]

		if ref.Name != name {
			continue
		}

		if nodeID != "" && ref.NodeID != nodeID {
			continue
		}

		return ref, true
	}

	return agentos.ArtifactRef{}, false
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}

	output := make(map[string]any, len(input))
	maps.Copy(output, input)

	return output
}
