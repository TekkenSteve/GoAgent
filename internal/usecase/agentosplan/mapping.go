package agentosplan

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ResolveRunInput applies plan/node input mappings and dereferences artifact refs
// before a backend-owned child run starts.
func ResolveRunInput(ctx context.Context, store ArtifactStore, expressions ValueExpressionCompiler, planInputs map[string]any, status agentos.RunPlanStatus, node agentos.PlanNodeSpec, edges []agentos.PlanEdgeSpec) (map[string]any, error) {
	resolved := cloneMap(node.Run.Input)
	mappings := make([]agentos.InputMapping, 0, len(node.Inputs)+len(edges))
	mappings = append(mappings, node.Inputs...)
	for _, edge := range edges {
		mappings = append(mappings, edge.InputMapping...)
	}
	if len(mappings) == 0 {
		return resolved, nil
	}

	data, err := json.Marshal(resolved)
	if err != nil {
		return nil, fmt.Errorf("%w: encode node input: %s", agentos.ErrInvalidRunPlan, err)
	}
	for _, mapping := range mappings {
		value, err := resolveMappingValue(ctx, store, expressions, planInputs, status, node, mapping)
		if err != nil {
			return nil, err
		}
		if value == nil && !mapping.Required {
			continue
		}
		data, err = sjson.SetBytes(data, mapping.Target, value)
		if err != nil {
			return nil, fmt.Errorf("%w: set input mapping %q: %s", agentos.ErrInvalidRunPlan, mapping.Target, err)
		}
	}

	var output map[string]any
	if err := json.Unmarshal(data, &output); err != nil {
		return nil, fmt.Errorf("%w: decode mapped input: %s", agentos.ErrInvalidRunPlan, err)
	}

	return output, nil
}

func resolveMappingValue(ctx context.Context, store ArtifactStore, expressions ValueExpressionCompiler, planInputs map[string]any, status agentos.RunPlanStatus, node agentos.PlanNodeSpec, mapping agentos.InputMapping) (any, error) {
	if mapping.Expression != "" {
		if expressions == nil {
			return nil, fmt.Errorf("%w: expression compiler is required for mapping %q", agentos.ErrInvalidExpression, mapping.Target)
		}
		compiled, err := expressions.CompileValue(mapping.Expression)
		if err != nil {
			return nil, err
		}

		return compiled.EvaluateValue(ctx, mappingVariables(planInputs, status, node))
	}
	if mapping.SourceArtifact != "" {
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
		_, payload, err := store.Get(ctx, agentos.PlanArtifactScope{
			PlanID:     status.PlanID,
			ArtifactID: ref.ArtifactID,
		})
		if err != nil {
			return nil, err
		}
		if payload == nil {
			return nil, fmt.Errorf("%w: artifact %q has no payload", agentos.ErrArtifactNotFound, mapping.SourceArtifact)
		}

		return selectSourcePath(payload, mapping.SourcePath, mapping.Required)
	}
	if mapping.SourcePath != "" {
		return selectSourcePath(planInputs, mapping.SourcePath, mapping.Required)
	}

	return nil, nil
}

func mappingVariables(planInputs map[string]any, status agentos.RunPlanStatus, node agentos.PlanNodeSpec) map[string]any {
	vars := Variables(agentos.RunPlanSpec{
		PlanID:   status.PlanID,
		Inputs:   planInputs,
		Metadata: status.Metadata,
	}, status)
	vars["node"] = node

	return vars
}

func selectSourcePath(source any, sourcePath string, required bool) (any, error) {
	if sourcePath == "" {
		return source, nil
	}
	data, err := json.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("%w: encode mapping source: %s", agentos.ErrInvalidRunPlan, err)
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

func findArtifact(refs []agentos.ArtifactRef, nodeID string, name string) (agentos.ArtifactRef, bool) {
	for _, ref := range refs {
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
	for key, value := range input {
		output[key] = value
	}

	return output
}
