package agentosplan

import (
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidateArtifactSpecs validates declared node output contracts.
func ValidateArtifactSpecs(nodeID string, specs []agentos.ArtifactSpec) error {
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		if spec.Name == "" {
			return artifactContractError(nodeID, "artifact name is required")
		}
		if spec.Kind == "" {
			return artifactContractError(nodeID, "artifact %q kind is required", spec.Name)
		}
		if _, exists := seen[spec.Name]; exists {
			return artifactContractError(nodeID, "duplicate artifact %q", spec.Name)
		}
		seen[spec.Name] = struct{}{}
	}

	return nil
}

// ValidateArtifactsAgainstSpecs validates published artifact refs against a
// node's declared output contract. An empty contract intentionally accepts any
// well-formed artifact refs so legacy-free callers can opt into strict outputs
// by declaring Outputs.
func ValidateArtifactsAgainstSpecs(nodeID string, specs []agentos.ArtifactSpec, refs []agentos.ArtifactRef) error {
	if err := ValidateArtifactSpecs(nodeID, specs); err != nil {
		return err
	}
	seenRefs := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if ref.Name == "" {
			return artifactContractError(nodeID, "published artifact without name")
		}
		if ref.Kind == "" {
			return artifactContractError(nodeID, "published artifact %q kind is required", ref.Name)
		}
		if _, exists := seenRefs[ref.Name]; exists {
			return artifactContractError(nodeID, "duplicate published artifact %q", ref.Name)
		}
		seenRefs[ref.Name] = struct{}{}
	}
	if len(specs) == 0 {
		return nil
	}

	specByName := make(map[string]agentos.ArtifactSpec, len(specs))
	for _, spec := range specs {
		specByName[spec.Name] = spec
	}
	for _, ref := range refs {
		spec, ok := specByName[ref.Name]
		if !ok {
			return artifactContractError(nodeID, "published undeclared artifact %q", ref.Name)
		}
		if ref.Kind != spec.Kind {
			return artifactContractError(nodeID, "artifact %q kind %q does not match declared kind %q", ref.Name, ref.Kind, spec.Kind)
		}
		if spec.MediaType != "" && ref.MediaType != spec.MediaType {
			return artifactContractError(nodeID, "artifact %q media type %q does not match declared media type %q", ref.Name, ref.MediaType, spec.MediaType)
		}
	}
	for _, spec := range specs {
		if !spec.Required {
			continue
		}
		if _, ok := seenRefs[spec.Name]; !ok {
			return artifactContractError(nodeID, "required output artifact %q was not published", spec.Name)
		}
	}

	return nil
}

// ValidateCapabilityOutputArtifacts validates published artifact refs against a
// capability output JSON Schema. The validated document shape is:
//
//	{"artifacts": [agentos.ArtifactRef JSON objects]}
func ValidateCapabilityOutputArtifacts(nodeID string, outputSchema json.RawMessage, refs []agentos.ArtifactRef) error {
	if len(outputSchema) == 0 {
		return nil
	}
	if err := validateRawSchema(outputSchema, ArtifactOutputDocument(refs)); err != nil {
		return artifactContractError(nodeID, "capability output schema: %s", err)
	}

	return nil
}

// ArtifactOutputDocument returns the stable JSON document shape validated by
// capability output schemas.
func ArtifactOutputDocument(refs []agentos.ArtifactRef) map[string]any {
	data, err := json.Marshal(refs)
	if err != nil {
		return map[string]any{"artifacts": []any{}}
	}
	var artifacts []map[string]any
	if err := json.Unmarshal(data, &artifacts); err != nil {
		return map[string]any{"artifacts": []any{}}
	}

	return map[string]any{"artifacts": artifacts}
}

func artifactContractError(nodeID, format string, args ...any) error {
	message := fmt.Sprintf(format, args...)
	if nodeID == "" {
		return fmt.Errorf("%w: %s", agentos.ErrInvalidArtifact, message)
	}

	return fmt.Errorf("%w: node %q %s", agentos.ErrInvalidArtifact, nodeID, message)
}
