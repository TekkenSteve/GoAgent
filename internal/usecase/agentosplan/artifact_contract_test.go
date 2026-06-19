package agentosplan

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestValidateArtifactSpecsRejectsDuplicateNames(t *testing.T) {
	err := ValidateArtifactSpecs("node-1", []agentos.ArtifactSpec{
		{Name: "summary", Kind: agentos.ArtifactKindObject},
		{Name: "summary", Kind: agentos.ArtifactKindText},
	})
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestValidateArtifactsAgainstSpecsRejectsUndeclaredAndMediaMismatch(t *testing.T) {
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
