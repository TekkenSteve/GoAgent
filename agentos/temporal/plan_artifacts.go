package temporal

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

const (
	COMPLETED = "completed"
	SUCCEEDED = "succeeded"
)

func normalizeRunArtifacts(planID string, node *agentos.PlanNodeSpec, status *agentos.RunStatus) ([]agentos.ArtifactRef, error) {
	refs := make([]agentos.ArtifactRef, 0, len(status.Artifacts))
	for i := range status.Artifacts {
		ref := status.Artifacts[i]

		normalizedRef, err := normalizeArtifactRef(planID, node, status, &ref)
		if err != nil {
			return nil, err
		}

		refs = append(refs, normalizedRef)
	}

	return refs, nil
}

func normalizeArtifactRef(planID string, node *agentos.PlanNodeSpec, status *agentos.RunStatus, ref *agentos.ArtifactRef) (agentos.ArtifactRef, error) {
	if ref.Name == "" {
		return agentos.ArtifactRef{}, fmt.Errorf("%w: node %q returned artifact without name", agentos.ErrInvalidArtifact, node.NodeID)
	}

	if ref.Kind == "" {
		return agentos.ArtifactRef{}, fmt.Errorf("%w: node %q artifact %q kind is required", agentos.ErrInvalidArtifact, node.NodeID, ref.Name)
	}

	if ref.ArtifactID == "" {
		return agentos.ArtifactRef{}, fmt.Errorf("%w: node %q artifact %q requires artifact id", agentos.ErrInvalidArtifact, node.NodeID, ref.Name)
	}

	if err := validateArtifactOwnership(planID, node, status, ref); err != nil {
		return agentos.ArtifactRef{}, err
	}

	applyArtifactOwnership(planID, node, status, ref)

	return *ref, nil
}

func validateArtifactOwnership(planID string, node *agentos.PlanNodeSpec, status *agentos.RunStatus, ref *agentos.ArtifactRef) error {
	if ref.PlanID != "" && ref.PlanID != planID {
		return fmt.Errorf("%w: node %q artifact %q belongs to plan %q", agentos.ErrInvalidArtifact, node.NodeID, ref.Name, ref.PlanID)
	}

	if ref.NodeID != "" && ref.NodeID != node.NodeID {
		return fmt.Errorf("%w: node %q artifact %q belongs to node %q", agentos.ErrInvalidArtifact, node.NodeID, ref.Name, ref.NodeID)
	}

	if ref.RunID != "" && status.RunID != "" && ref.RunID != status.RunID {
		return fmt.Errorf("%w: node %q artifact %q belongs to run %q", agentos.ErrInvalidArtifact, node.NodeID, ref.Name, ref.RunID)
	}

	return nil
}

func applyArtifactOwnership(planID string, node *agentos.PlanNodeSpec, status *agentos.RunStatus, ref *agentos.ArtifactRef) {
	if ref.PlanID == "" {
		ref.PlanID = planID
	}

	if ref.NodeID == "" {
		ref.NodeID = node.NodeID
	}

	if ref.RunID == "" {
		ref.RunID = status.RunID
	}
}

func validateRequiredArtifacts(outputs []agentos.ArtifactSpec, refs []agentos.ArtifactRef) error {
	return agentosplan.ValidateArtifactsAgainstSpecs("", outputs, refs)
}

func runSucceeded(lifecycle string) bool {
	switch lifecycle {
	case COMPLETED, SUCCEEDED:
		return true
	default:
		return false
	}
}
