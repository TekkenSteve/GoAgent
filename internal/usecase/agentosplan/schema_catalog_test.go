package agentosplan

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestArtifactSchemaRegistrationUsesCanonicalJSON(t *testing.T) {
	t.Parallel()

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

	firstKey := artifactSchemaRegistrationKeyForTest(t, first, "first")
	reorderedKey := artifactSchemaRegistrationKeyForTest(t, reordered, "reordered")

	if firstKey != reorderedKey {
		t.Fatalf("canonical keys differ: %q != %q", firstKey, reorderedKey)
	}

	catalog, err := NewStaticArtifactSchemaCatalog(nil)
	if err != nil {
		t.Fatalf("NewStaticArtifactSchemaCatalog: %v", err)
	}

	registerArtifactSchemaForTest(t, catalog, first, firstKey, true)

	replayed := registerArtifactSchemaForTest(t, catalog, reordered, reorderedKey, false)
	if replayed.Ref != first.Ref {
		t.Fatalf("canonical replay = %#v", replayed)
	}

	changed := reordered
	changed.Schema = json.RawMessage(`{"type":"object","required":["title"]}`)

	changedKey := artifactSchemaRegistrationKeyForTest(t, changed, "changed")

	_, _, err = catalog.RegisterArtifactSchema(context.Background(), changed, changedKey)
	if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
		t.Fatalf("RegisterArtifactSchema changed error = %v, want ErrInvalidArtifact", err)
	}
}

func artifactSchemaRegistrationKeyForTest(t *testing.T, schema agentos.ArtifactSchema, label string) string {
	t.Helper()

	key, err := ArtifactSchemaRegistrationIdempotencyKey(schema)
	if err != nil {
		t.Fatalf("ArtifactSchemaRegistrationIdempotencyKey %s: %v", label, err)
	}

	return key
}

func registerArtifactSchemaForTest(t *testing.T, catalog *StaticArtifactSchemaCatalog, schema agentos.ArtifactSchema, key string, wantCreated bool) agentos.ArtifactSchema {
	t.Helper()

	registered, created, err := catalog.RegisterArtifactSchema(context.Background(), schema, key)
	if err != nil {
		t.Fatalf("RegisterArtifactSchema: %v", err)
	}

	if created != wantCreated {
		t.Fatalf("registration created = %v, want %v, registered=%#v", created, wantCreated, registered)
	}

	return registered
}
