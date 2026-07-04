package agentosplan

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

var errCanonicalJSONMultipleValues = errors.New("contains multiple JSON values")

// StaticArtifactSchemaCatalog is an explicit in-memory schema catalog for
// tests, CLI validation, and embedded setups that do not need a durable schema
// registry.
type StaticArtifactSchemaCatalog struct {
	mu      sync.RWMutex
	schemas map[string]agentos.ArtifactSchema
}

// NewStaticArtifactSchemaCatalog creates a catalog from public schema
// declarations.
func NewStaticArtifactSchemaCatalog(schemas []agentos.ArtifactSchema) (*StaticArtifactSchemaCatalog, error) {
	catalog := &StaticArtifactSchemaCatalog{schemas: make(map[string]agentos.ArtifactSchema, len(schemas))}
	for _, schema := range schemas {
		if _, _, err := catalog.RegisterArtifactSchema(context.Background(), schema, ""); err != nil {
			return nil, err
		}
	}

	return catalog, nil
}

// NormalizeArtifactSchema checks one schema declaration and returns its
// canonical JSON representation for stable hashing and durable storage.
func NormalizeArtifactSchema(schema agentos.ArtifactSchema) (agentos.ArtifactSchema, error) {
	if schema.Ref == "" {
		return agentos.ArtifactSchema{}, fmt.Errorf("%w: artifact schema ref is required", agentoscore.ErrInvalidArtifact)
	}

	if len(schema.Schema) == 0 {
		return agentos.ArtifactSchema{}, fmt.Errorf("%w: artifact schema %q is empty", agentoscore.ErrInvalidArtifact, schema.Ref)
	}

	normalizedSchema, err := canonicalRawJSON(schema.Schema)
	if err != nil {
		return agentos.ArtifactSchema{}, fmt.Errorf("%w: artifact schema %q: %w", agentoscore.ErrInvalidArtifact, schema.Ref, err)
	}

	if err := validateRawSchemaSyntax(normalizedSchema); err != nil {
		return agentos.ArtifactSchema{}, fmt.Errorf("%w: artifact schema %q: %w", agentoscore.ErrInvalidArtifact, schema.Ref, err)
	}

	schema.Schema = normalizedSchema

	return cloneArtifactSchema(schema), nil
}

// ValidateArtifactSchema checks one schema declaration before registration.
func ValidateArtifactSchema(schema agentos.ArtifactSchema) error {
	_, err := NormalizeArtifactSchema(schema)

	return err
}

// RegisterArtifactSchemas registers a batch of schema declarations using
// content-addressed idempotency keys.
func RegisterArtifactSchemas(ctx context.Context, registry ArtifactSchemaRegistry, schemas []agentos.ArtifactSchema) error {
	if registry == nil {
		return fmt.Errorf("%w: artifact schema registry is required", agentoscore.ErrInvalidArtifact)
	}

	for _, schema := range schemas {
		key, err := ArtifactSchemaRegistrationIdempotencyKey(schema)
		if err != nil {
			return err
		}

		if _, _, err := registry.RegisterArtifactSchema(ctx, schema, key); err != nil {
			return err
		}
	}

	return nil
}

// ArtifactSchemaRegistrationIdempotencyKey returns a stable key for one schema
// declaration. A changed schema intentionally produces a different key.
func ArtifactSchemaRegistrationIdempotencyKey(schema agentos.ArtifactSchema) (string, error) {
	data, err := artifactSchemaDeclarationJSON(schema)
	if err != nil {
		return "", fmt.Errorf("%w: marshal artifact schema registration: %w", agentoscore.ErrInvalidArtifact, err)
	}

	sum := sha256.Sum256(data)

	return "artifact_schema:" + hex.EncodeToString(sum[:]), nil
}

// ValidateArtifactSchemaRegistrationIdempotency rejects replaying one
// idempotency key for a different schema declaration.
func ValidateArtifactSchemaRegistrationIdempotency(existing, requested agentos.ArtifactSchema) error {
	existingJSON, err := artifactSchemaDeclarationJSON(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing artifact schema: %w", agentoscore.ErrInvalidArtifact, err)
	}

	requestedJSON, err := artifactSchemaDeclarationJSON(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested artifact schema: %w", agentoscore.ErrInvalidArtifact, err)
	}

	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: artifact schema registration idempotency key reused with different declaration", agentoscore.ErrInvalidArtifact)
	}

	return nil
}

// RegisterArtifactSchema adds or replaces one schema declaration.
func (c *StaticArtifactSchemaCatalog) RegisterArtifactSchema(_ context.Context, schema agentos.ArtifactSchema, idempotencyKey string) (agentos.ArtifactSchema, bool, error) {
	if c == nil {
		return agentos.ArtifactSchema{}, false, fmt.Errorf("%w: artifact schema catalog is nil", agentoscore.ErrInvalidArtifact)
	}

	normalized, err := NormalizeArtifactSchema(schema)
	if err != nil {
		return agentos.ArtifactSchema{}, false, err
	}

	if idempotencyKey != "" {
		expectedKey, err := ArtifactSchemaRegistrationIdempotencyKey(normalized)
		if err != nil {
			return agentos.ArtifactSchema{}, false, err
		}

		if idempotencyKey != expectedKey {
			return agentos.ArtifactSchema{}, false, fmt.Errorf("%w: artifact schema registration idempotency key must match declaration", agentoscore.ErrInvalidArtifact)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	existing, exists := c.schemas[normalized.Ref]
	if exists {
		if err := ValidateArtifactSchemaRegistrationIdempotency(existing, normalized); err != nil {
			return agentos.ArtifactSchema{}, false, err
		}

		return existing, false, nil
	}

	c.schemas[normalized.Ref] = cloneArtifactSchema(normalized)

	return normalized, true, nil
}

// GetArtifactSchema returns a copy of the schema document for schemaRef.
func (c *StaticArtifactSchemaCatalog) GetArtifactSchema(_ context.Context, schemaRef string) (json.RawMessage, bool, error) {
	if c == nil {
		return nil, false, nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[schemaRef]
	if !ok {
		return nil, false, nil
	}

	return append(json.RawMessage(nil), schema.Schema...), true, nil
}

func cloneArtifactSchema(schema agentos.ArtifactSchema) agentos.ArtifactSchema {
	schema.Schema = append(json.RawMessage(nil), schema.Schema...)

	return schema
}

func artifactSchemaDeclarationJSON(schema agentos.ArtifactSchema) ([]byte, error) {
	normalized, err := NormalizeArtifactSchema(schema)
	if err != nil {
		return nil, err
	}

	return json.Marshal(normalized)
}

func canonicalRawJSON(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errCanonicalJSONMultipleValues
		}

		return nil, err
	}

	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(data), nil
}

var (
	_ ArtifactSchemaCatalog  = (*StaticArtifactSchemaCatalog)(nil)
	_ ArtifactSchemaRegistry = (*StaticArtifactSchemaCatalog)(nil)
)
