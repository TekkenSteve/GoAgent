package agentosplan

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestArtifactSchemaRegistrationUsesCanonicalJSON(t *testing.T) {
	first := agentos.ArtifactSchema{
		Ref:         "schema:summary",
		Description: "Summary artifact",
		Schema: json.RawMessage(`{
			"type": "object",
			"properties": {"score": {"type": "number"}},
			"required": ["score"]
		}`),
	}
	reordered := agentos.ArtifactSchema{
		Ref:         "schema:summary",
		Description: "Summary artifact",
		Schema:      json.RawMessage(`{"required":["score"],"properties":{"score":{"type":"number"}},"type":"object"}`),
	}

	firstKey, err := ArtifactSchemaRegistrationIdempotencyKey(first)
	if err != nil {
		t.Fatalf("ArtifactSchemaRegistrationIdempotencyKey first: %v", err)
	}
	reorderedKey, err := ArtifactSchemaRegistrationIdempotencyKey(reordered)
	if err != nil {
		t.Fatalf("ArtifactSchemaRegistrationIdempotencyKey reordered: %v", err)
	}
	if firstKey != reorderedKey {
		t.Fatalf("canonical keys differ: %q != %q", firstKey, reorderedKey)
	}

	catalog, err := NewStaticArtifactSchemaCatalog(nil)
	if err != nil {
		t.Fatalf("NewStaticArtifactSchemaCatalog: %v", err)
	}
	registered, created, err := catalog.RegisterArtifactSchema(context.Background(), first, firstKey)
	if err != nil {
		t.Fatalf("RegisterArtifactSchema first: %v", err)
	}
	if !created {
		t.Fatalf("first registration created = false, registered=%#v", registered)
	}
	replayed, created, err := catalog.RegisterArtifactSchema(context.Background(), reordered, reorderedKey)
	if err != nil {
		t.Fatalf("RegisterArtifactSchema canonical replay: %v", err)
	}
	if created || replayed.Ref != first.Ref {
		t.Fatalf("canonical replay = %#v created=%v", replayed, created)
	}

	changed := reordered
	changed.Schema = json.RawMessage(`{"type":"object","required":["title"]}`)
	changedKey, err := ArtifactSchemaRegistrationIdempotencyKey(changed)
	if err != nil {
		t.Fatalf("ArtifactSchemaRegistrationIdempotencyKey changed: %v", err)
	}
	_, _, err = catalog.RegisterArtifactSchema(context.Background(), changed, changedKey)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("RegisterArtifactSchema changed error = %v, want ErrInvalidArtifact", err)
	}
}
