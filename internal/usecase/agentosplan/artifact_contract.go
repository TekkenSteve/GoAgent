package agentosplan

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidateArtifactSpecs validates declared node output contracts.
func ValidateArtifactSpecs(nodeID string, specs []agentos.ArtifactSpec) error {
	seen := make(map[string]struct{}, len(specs))
	for i := range specs {
		spec := specs[i]
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

	seenRefs, err := validatePublishedArtifacts(nodeID, refs)
	if err != nil {
		return err
	}

	if len(specs) == 0 {
		return nil
	}

	specByName := buildSpecByName(specs)

	if err := matchRefsAgainstSpecs(nodeID, specByName, refs); err != nil {
		return err
	}

	return checkRequiredArtifacts(nodeID, specByName, seenRefs)
}

func validatePublishedArtifacts(nodeID string, refs []agentos.ArtifactRef) (map[string]struct{}, error) {
	seenRefs := make(map[string]struct{}, len(refs))
	for i := range refs {
		ref := refs[i]
		if ref.Name == "" {
			return nil, artifactContractError(nodeID, "published artifact without name")
		}

		if ref.Kind == "" {
			return nil, artifactContractError(nodeID, "published artifact %q kind is required", ref.Name)
		}

		if _, exists := seenRefs[ref.Name]; exists {
			return nil, artifactContractError(nodeID, "duplicate published artifact %q", ref.Name)
		}

		seenRefs[ref.Name] = struct{}{}
	}

	return seenRefs, nil
}

func buildSpecByName(specs []agentos.ArtifactSpec) map[string]agentos.ArtifactSpec {
	specByName := make(map[string]agentos.ArtifactSpec, len(specs))
	for i := range specs {
		spec := specs[i]
		specByName[spec.Name] = spec
	}

	return specByName
}

func matchRefsAgainstSpecs(nodeID string, specByName map[string]agentos.ArtifactSpec, refs []agentos.ArtifactRef) error {
	for i := range refs {
		ref := refs[i]

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

	return nil
}

func checkRequiredArtifacts(nodeID string, specByName map[string]agentos.ArtifactSpec, seenRefs map[string]struct{}) error {
	for _, spec := range specByName {
		if !spec.Required {
			continue
		}

		if _, ok := seenRefs[spec.Name]; !ok {
			return artifactContractError(nodeID, "required output artifact %q was not published", spec.Name)
		}
	}

	return nil
}

// ValidateArtifactSchemaRefs verifies that every declared schema_ref resolves
// to a valid JSON Schema document.
func ValidateArtifactSchemaRefs(ctx context.Context, catalog ArtifactSchemaCatalog, nodeID string, specs []agentos.ArtifactSpec) error {
	for i := range specs {
		spec := specs[i]
		if spec.SchemaRef == "" {
			continue
		}

		raw, err := artifactSchema(ctx, catalog, nodeID, spec)
		if err != nil {
			return err
		}

		if err := validateRawSchemaSyntax(raw); err != nil {
			return artifactContractError(nodeID, "artifact %q schema_ref %q is invalid: %s", spec.Name, spec.SchemaRef, err)
		}
	}

	return nil
}

// ValidateArtifactPayloadsAgainstSchemas validates stored artifact payloads
// against their declared schema_ref contracts.
func ValidateArtifactPayloadsAgainstSchemas(ctx context.Context, store ArtifactStore, catalog ArtifactSchemaCatalog, plan *agentos.RunPlanSpec, node *agentos.PlanNodeSpec, refs []agentos.ArtifactRef) error {
	for _, spec := range node.Outputs {
		if spec.SchemaRef == "" {
			continue
		}

		err := validateArtifactPayloadAgainstSchema(ctx, store, catalog, plan, node, &spec, refs)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateArtifactPayloadAgainstSchema(ctx context.Context, store ArtifactStore, catalog ArtifactSchemaCatalog, plan *agentos.RunPlanSpec, node *agentos.PlanNodeSpec, spec *agentos.ArtifactSpec, refs []agentos.ArtifactRef) error {
	ref, ok := findArtifact(refs, "", spec.Name)
	if !ok {
		return nil
	}

	if ref.ArtifactID == "" {
		return artifactContractError(node.NodeID, "artifact %q schema_ref %q requires an artifact id", spec.Name, spec.SchemaRef)
	}

	if store == nil {
		return artifactContractError(node.NodeID, "artifact %q schema_ref %q requires an artifact store", spec.Name, spec.SchemaRef)
	}

	raw, err := artifactSchema(ctx, catalog, node.NodeID, *spec)
	if err != nil {
		return err
	}

	payload, err := storedArtifactPayload(ctx, store, plan, &ref)
	if err != nil {
		return err
	}

	if payload == nil {
		return artifactContractError(node.NodeID, "artifact %q schema_ref %q requires stored payload", spec.Name, spec.SchemaRef)
	}

	if err := validateRawSchema(raw, payload); err != nil {
		return artifactContractError(node.NodeID, "artifact %q payload does not match schema_ref %q: %s", spec.Name, spec.SchemaRef, err)
	}

	return nil
}

func storedArtifactPayload(ctx context.Context, store ArtifactStore, plan *agentos.RunPlanSpec, ref *agentos.ArtifactRef) (any, error) {
	scope := agentos.PlanArtifactScope{
		PlanID:     plan.PlanID,
		AccountID:  plan.AccountID,
		ProjectID:  plan.ProjectID,
		ArtifactID: ref.ArtifactID,
	}

	_, payload, err := store.Get(ctx, &scope)
	if err != nil {
		return nil, err
	}

	return payload, nil
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

func artifactSchema(ctx context.Context, catalog ArtifactSchemaCatalog, nodeID string, spec agentos.ArtifactSpec) (json.RawMessage, error) {
	if catalog == nil {
		return nil, artifactContractError(nodeID, "artifact %q schema_ref %q requires an artifact schema catalog", spec.Name, spec.SchemaRef)
	}

	raw, ok, err := catalog.GetArtifactSchema(ctx, spec.SchemaRef)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, artifactContractError(nodeID, "artifact %q schema_ref %q was not found", spec.Name, spec.SchemaRef)
	}

	return raw, nil
}
