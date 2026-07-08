package agentosplan

import (
	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// Variables returns the fixed CEL environment for plan evaluation.
func Variables(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) map[string]any {
	artifactsByNode := make(map[string][]agentoscore.ArtifactRef)

	for i := range status.Artifacts {
		artifact := status.Artifacts[i]

		if artifact.NodeID == "" {
			continue
		}

		artifactsByNode[artifact.NodeID] = append(artifactsByNode[artifact.NodeID], artifact)
	}

	return map[string]any{
		"plan":      *spec,
		"inputs":    spec.Inputs,
		"outputs":   artifactsByNode,
		"artifacts": artifactsByNode,
		"metadata":  spec.Metadata,
		"status":    *status,
	}
}
