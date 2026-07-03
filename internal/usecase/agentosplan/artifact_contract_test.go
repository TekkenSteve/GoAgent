package agentosplan

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestValidateArtifactSpecsRejectsDuplicateNames(t *testing.T) {
	t.Parallel()

	err := ValidateArtifactSpecs("node-1", []agentos.ArtifactSpec{
		{Name: "summary", Kind: agentos.ArtifactKindObject},
		{Name: "summary", Kind: agentos.ArtifactKindText},
	})
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestValidateArtifactsAgainstSpecsRejectsUndeclaredAndMediaMismatch(t *testing.T) {
	t.Parallel()

	specs := []agentos.ArtifactSpec{
		{Name: "summary", Kind: agentos.ArtifactKindObject, MediaType: "application/json", Required: true},
	}

	err := ValidateArtifactsAgainstSpecs("node-1", specs, []agentos.ArtifactRef{
		{Name: "summary", Kind: agentos.ArtifactKindObject, MediaType: "text/plain"},
	})
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("media mismatch error = %v, want ErrInvalidArtifact", err)
	}

	err = ValidateArtifactsAgainstSpecs("node-1", specs, []agentos.ArtifactRef{
		{Name: "summary", Kind: agentos.ArtifactKindObject, MediaType: "application/json"},
		{Name: "extra", Kind: agentos.ArtifactKindText},
	})
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("undeclared error = %v, want ErrInvalidArtifact", err)
	}
}

func TestValidateCapabilityOutputArtifactsUsesStableDocumentShape(t *testing.T) {
	t.Parallel()

	schema := json.RawMessage(`{
		"type": "object",
		"properties": {
			"artifacts": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"name": {"enum": ["summary"]},
						"metadata": {
							"type": "object",
							"properties": {
								"quality": {"enum": ["approved"]}
							},
							"required": ["quality"]
						}
					},
					"required": ["name", "metadata"]
				}
			}
		},
		"required": ["artifacts"]
	}`)

	err := ValidateCapabilityOutputArtifacts("node-1", schema, []agentos.ArtifactRef{
		{Name: "summary", Kind: agentos.ArtifactKindObject, Metadata: map[string]string{"quality": "draft"}},
	})
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("schema mismatch error = %v, want ErrInvalidArtifact", err)
	}

	if err := ValidateCapabilityOutputArtifacts("node-1", schema, []agentos.ArtifactRef{
		{Name: "summary", Kind: agentos.ArtifactKindObject, Metadata: map[string]string{"quality": "approved"}},
	}); err != nil {
		t.Fatalf("schema match error = %v", err)
	}
}

func TestValidateArtifactSchemaRefsRequiresCatalog(t *testing.T) {
	t.Parallel()

	specs := []agentos.ArtifactSpec{{Name: "summary", Kind: agentos.ArtifactKindObject, SchemaRef: "schema:summary"}}

	err := ValidateArtifactSchemaRefs(context.Background(), nil, "node-1", specs)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("missing catalog error = %v, want ErrInvalidArtifact", err)
	}

	catalog, err := NewStaticArtifactSchemaCatalog([]agentos.ArtifactSchema{
		{Ref: "schema:summary", Schema: json.RawMessage(`{"type":"object"}`)},
	})
	if err != nil {
		t.Fatalf("NewStaticArtifactSchemaCatalog: %v", err)
	}

	if err := ValidateArtifactSchemaRefs(context.Background(), catalog, "node-1", specs); err != nil {
		t.Fatalf("ValidateArtifactSchemaRefs: %v", err)
	}
}

func TestValidateArtifactPayloadsAgainstSchemas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	catalog, err := NewStaticArtifactSchemaCatalog([]agentos.ArtifactSchema{
		{Ref: "schema:summary", Schema: json.RawMessage(`{
			"type": "object",
			"properties": {"score": {"type": "number"}},
			"required": ["score"]
		}`)},
	})
	if err != nil {
		t.Fatalf("NewStaticArtifactSchemaCatalog: %v", err)
	}

	store := NewMemoryArtifactStore()
	plan := agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}
	node := agentos.PlanNodeSpec{
		NodeID: "node-1",
		Outputs: []agentos.ArtifactSpec{
			{Name: "summary", Kind: agentos.ArtifactKindObject, SchemaRef: "schema:summary"},
		},
	}

	badRef, err := putArtifact(ctx, store, &agentos.ArtifactRef{
		ArtifactID: "artifact-bad",
		PlanID:     plan.PlanID,
		NodeID:     node.NodeID,
		RunID:      "run-1",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"score": "high"}, "artifact-bad-key")
	if err != nil {
		t.Fatalf("Put bad artifact: %v", err)
	}

	err = ValidateArtifactPayloadsAgainstSchemas(ctx, store, catalog, &plan, &node, []agentos.ArtifactRef{badRef})
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("schema mismatch error = %v, want ErrInvalidArtifact", err)
	}

	goodRef, err := putArtifact(ctx, store, &agentos.ArtifactRef{
		ArtifactID: "artifact-good",
		PlanID:     plan.PlanID,
		NodeID:     node.NodeID,
		RunID:      "run-1",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"score": 1.0}, "artifact-good-key")
	if err != nil {
		t.Fatalf("Put good artifact: %v", err)
	}

	if err := ValidateArtifactPayloadsAgainstSchemas(ctx, store, catalog, &plan, &node, []agentos.ArtifactRef{goodRef}); err != nil {
		t.Fatalf("ValidateArtifactPayloadsAgainstSchemas: %v", err)
	}
}
