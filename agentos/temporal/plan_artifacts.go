package temporal

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func normalizeRunArtifacts(planID string, node agentos.PlanNodeSpec, status agentos.RunStatus) ([]agentos.ArtifactRef, error) {
	refs := make([]agentos.ArtifactRef, 0, len(status.Artifacts))
	for _, ref := range status.Artifacts {
		if ref.Name == "" {
			return nil, fmt.Errorf("%w: node %q returned artifact without name", agentos.ErrInvalidArtifact, node.NodeID)
		}
		if ref.Kind == "" {
			return nil, fmt.Errorf("%w: node %q artifact %q kind is required", agentos.ErrInvalidArtifact, node.NodeID, ref.Name)
		}
		if ref.ArtifactID == "" && ref.URI == "" {
			return nil, fmt.Errorf("%w: node %q artifact %q requires artifact id or uri", agentos.ErrInvalidArtifact, node.NodeID, ref.Name)
		}
		if ref.PlanID != "" && ref.PlanID != planID {
			return nil, fmt.Errorf("%w: node %q artifact %q belongs to plan %q", agentos.ErrInvalidArtifact, node.NodeID, ref.Name, ref.PlanID)
		}
		if ref.PlanID == "" {
			ref.PlanID = planID
		}
		if ref.NodeID != "" && ref.NodeID != node.NodeID {
			return nil, fmt.Errorf("%w: node %q artifact %q belongs to node %q", agentos.ErrInvalidArtifact, node.NodeID, ref.Name, ref.NodeID)
		}
		if ref.NodeID == "" {
			ref.NodeID = node.NodeID
		}
		if ref.RunID != "" && status.RunID != "" && ref.RunID != status.RunID {
			return nil, fmt.Errorf("%w: node %q artifact %q belongs to run %q", agentos.ErrInvalidArtifact, node.NodeID, ref.Name, ref.RunID)
		}
		if ref.RunID == "" {
			ref.RunID = status.RunID
		}
		refs = append(refs, ref)
	}

	return refs, nil
}

func validateRequiredArtifacts(outputs []agentos.ArtifactSpec, refs []agentos.ArtifactRef) error {
	return agentosplan.ValidateArtifactsAgainstSpecs("", outputs, refs)
}

func runSucceeded(lifecycle string) bool {
	switch lifecycle {
	case "completed", "succeeded":
		return true
	default:
		return false
	}
}
