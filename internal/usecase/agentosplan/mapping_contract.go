package agentosplan

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

type inputMappingSourceMode string

const (
	inputMappingSourceExpression inputMappingSourceMode = "expression"
	inputMappingSourceArtifact   inputMappingSourceMode = "artifact"
	inputMappingSourcePlanInput  inputMappingSourceMode = "plan_input"
)

func validateInputMappingShape(scope string, mapping *agentos.InputMapping) (inputMappingSourceMode, error) {
	if mapping.Target == "" {
		return "", fmt.Errorf("%w: %s input mapping target is required", agentos.ErrInvalidRunPlan, scope)
	}

	hasExpression := mapping.Expression != ""
	hasArtifact := mapping.SourceArtifact != ""
	hasSourceNode := mapping.SourceNodeID != ""
	hasSourcePath := mapping.SourcePath != ""

	if hasExpression {
		if hasArtifact || hasSourceNode || hasSourcePath {
			return "", fmt.Errorf("%w: %s input mapping %q mixes expression with source fields", agentos.ErrInvalidRunPlan, scope, mapping.Target)
		}

		return inputMappingSourceExpression, nil
	}

	if hasArtifact {
		if !hasSourceNode {
			return "", fmt.Errorf("%w: %s input mapping %q source_node_id is required for source_artifact %q", agentos.ErrInvalidArtifact, scope, mapping.Target, mapping.SourceArtifact)
		}

		return inputMappingSourceArtifact, nil
	}

	if hasSourceNode {
		return "", fmt.Errorf("%w: %s input mapping %q source_node_id requires source_artifact", agentos.ErrInvalidRunPlan, scope, mapping.Target)
	}

	if hasSourcePath {
		return inputMappingSourcePlanInput, nil
	}

	return "", fmt.Errorf("%w: %s input mapping %q source is required", agentos.ErrInvalidRunPlan, scope, mapping.Target)
}
