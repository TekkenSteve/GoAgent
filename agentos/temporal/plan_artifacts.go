package temporal

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
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
		if ref.PlanID == "" {
			ref.PlanID = planID
		}
		if ref.NodeID == "" {
			ref.NodeID = node.NodeID
		}
		if ref.RunID == "" {
			ref.RunID = status.RunID
		}
		refs = append(refs, ref)
	}

	return refs, nil
}

func validateRequiredArtifacts(outputs []agentos.ArtifactSpec, refs []agentos.ArtifactRef) error {
	byName := make(map[string]agentos.ArtifactRef, len(refs))
	for _, ref := range refs {
		byName[ref.Name] = ref
	}
	for _, output := range outputs {
		if !output.Required {
			continue
		}
		ref, ok := byName[output.Name]
		if !ok {
			return fmt.Errorf("%w: required output artifact %q was not published", agentos.ErrInvalidArtifact, output.Name)
		}
		if ref.Kind != output.Kind {
			return fmt.Errorf("%w: output artifact %q kind %q does not match required kind %q", agentos.ErrInvalidArtifact, output.Name, ref.Kind, output.Kind)
		}
	}

	return nil
}
