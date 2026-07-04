package temporal

import (
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

const (
	COMPLETED = "completed"
	SUCCEEDED = "succeeded"
)

func normalizeRunArtifacts(planID string, node *agentos.PlanNodeSpec, status *agentos.RunStatus) ([]agentoscore.ArtifactRef, error) {
	refs := make([]agentoscore.ArtifactRef, 0, len(status.Artifacts))
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

func normalizeArtifactRef(planID string, node *agentos.PlanNodeSpec, status *agentos.RunStatus, ref *agentoscore.ArtifactRef) (agentoscore.ArtifactRef, error) {
	if ref.Name == "" {
		return agentoscore.ArtifactRef{}, fmt.Errorf("%w: node %q returned artifact without name", agentoscore.ErrInvalidArtifact, node.NodeID)
	}

	if ref.Kind == "" {
		return agentoscore.ArtifactRef{}, fmt.Errorf("%w: node %q artifact %q kind is required", agentoscore.ErrInvalidArtifact, node.NodeID, ref.Name)
	}

	if ref.ArtifactID == "" {
		return agentoscore.ArtifactRef{}, fmt.Errorf("%w: node %q artifact %q requires artifact id", agentoscore.ErrInvalidArtifact, node.NodeID, ref.Name)
	}

	if err := validateArtifactOwnership(planID, node, status, ref); err != nil {
		return agentoscore.ArtifactRef{}, err
	}

	applyArtifactOwnership(planID, node, status, ref)

	return *ref, nil
}

func validateArtifactOwnership(planID string, node *agentos.PlanNodeSpec, status *agentos.RunStatus, ref *agentoscore.ArtifactRef) error {
	if ref.PlanID != "" && ref.PlanID != planID {
		return fmt.Errorf("%w: node %q artifact %q belongs to plan %q", agentoscore.ErrInvalidArtifact, node.NodeID, ref.Name, ref.PlanID)
	}

	if ref.NodeID != "" && ref.NodeID != node.NodeID {
		return fmt.Errorf("%w: node %q artifact %q belongs to node %q", agentoscore.ErrInvalidArtifact, node.NodeID, ref.Name, ref.NodeID)
	}

	if ref.RunID != "" && status.RunID != "" && ref.RunID != status.RunID {
		return fmt.Errorf("%w: node %q artifact %q belongs to run %q", agentoscore.ErrInvalidArtifact, node.NodeID, ref.Name, ref.RunID)
	}

	return nil
}

func applyArtifactOwnership(planID string, node *agentos.PlanNodeSpec, status *agentos.RunStatus, ref *agentoscore.ArtifactRef) {
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

func validateRequiredArtifacts(outputs []agentos.ArtifactSpec, refs []agentoscore.ArtifactRef) error {
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
