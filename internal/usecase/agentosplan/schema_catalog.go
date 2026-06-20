package agentosplan

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// StaticArtifactSchemaCatalog is an explicit in-memory schema catalog for
// tests, CLI validation, and embedded setups that do not need a durable schema
// registry.
type StaticArtifactSchemaCatalog struct {
	schemas map[string]json.RawMessage
}

// NewStaticArtifactSchemaCatalog creates a catalog from schema refs to JSON
// Schema documents.
func NewStaticArtifactSchemaCatalog(schemas map[string]json.RawMessage) (*StaticArtifactSchemaCatalog, error) {
	catalog := &StaticArtifactSchemaCatalog{schemas: make(map[string]json.RawMessage, len(schemas))}
	for ref, raw := range schemas {
		if ref == "" {
			return nil, fmt.Errorf("%w: artifact schema ref is required", agentos.ErrInvalidArtifact)
		}
		if len(raw) == 0 {
			return nil, fmt.Errorf("%w: artifact schema %q is empty", agentos.ErrInvalidArtifact, ref)
		}
		if err := validateRawSchemaSyntax(raw); err != nil {
			return nil, fmt.Errorf("%w: artifact schema %q: %s", agentos.ErrInvalidArtifact, ref, err)
		}
		catalog.schemas[ref] = append(json.RawMessage(nil), raw...)
	}

	return catalog, nil
}

// GetArtifactSchema returns a copy of the schema document for schemaRef.
func (c *StaticArtifactSchemaCatalog) GetArtifactSchema(_ context.Context, schemaRef string) (json.RawMessage, bool, error) {
	if c == nil {
		return nil, false, nil
	}
	raw, ok := c.schemas[schemaRef]
	if !ok {
		return nil, false, nil
	}

	return append(json.RawMessage(nil), raw...), true, nil
}

var _ ArtifactSchemaCatalog = (*StaticArtifactSchemaCatalog)(nil)
