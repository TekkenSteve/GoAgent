package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/jackc/pgx/v5"
)

// AgentOSArtifactSchemaCatalogRepo persists artifact JSON Schema declarations.
// Statements and bindings come from queries/artifact_schema_catalog.sql; this
// file owns the registration idempotency protocol around them.
type AgentOSArtifactSchemaCatalogRepo struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

// NewAgentOSArtifactSchemaCatalogRepo creates a Postgres-backed artifact schema catalog.
func NewAgentOSArtifactSchemaCatalogRepo(pg *postgres.Postgres) *AgentOSArtifactSchemaCatalogRepo {
	return &AgentOSArtifactSchemaCatalogRepo{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
}

// RegisterArtifactSchema upserts an artifact JSON Schema declaration, returning the stored schema and whether it was newly registered.
func (r *AgentOSArtifactSchemaCatalogRepo) RegisterArtifactSchema(ctx context.Context, schema agentos.ArtifactSchema, idempotencyKey string) (agentos.ArtifactSchema, bool, error) {
	normalized, err := agentosplan.NormalizeArtifactSchema(schema)
	if err != nil {
		return agentos.ArtifactSchema{}, false, err
	}

	if err := validateArtifactSchemaIdempotencyKey(normalized, idempotencyKey); err != nil {
		return agentos.ArtifactSchema{}, false, err
	}

	existing, exists, err := r.schemaByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return agentos.ArtifactSchema{}, false, err
	}

	if exists {
		if err := agentosplan.ValidateArtifactSchemaRegistrationIdempotency(existing, normalized); err != nil {
			return agentos.ArtifactSchema{}, false, err
		}

		return existing, false, nil
	}

	existing, exists, err = r.schemaByRef(ctx, normalized.Ref)
	if err != nil {
		return agentos.ArtifactSchema{}, false, err
	}

	if exists {
		if err := agentosplan.ValidateArtifactSchemaRegistrationIdempotency(existing, normalized); err != nil {
			return agentos.ArtifactSchema{}, false, err
		}
	}

	registered, err := r.insertSchemaRow(ctx, normalized, idempotencyKey)
	if err != nil {
		return agentos.ArtifactSchema{}, false, err
	}

	return registered, !exists, nil
}

func (r *AgentOSArtifactSchemaCatalogRepo) insertSchemaRow(ctx context.Context, normalized agentos.ArtifactSchema, idempotencyKey string) (agentos.ArtifactSchema, error) {
	schemaJSON, err := json.Marshal(normalized.Schema)
	if err != nil {
		return agentos.ArtifactSchema{}, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - marshal schema: %w", err)
	}

	declarationJSON, err := json.Marshal(normalized)
	if err != nil {
		return agentos.ArtifactSchema{}, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - marshal declaration: %w", err)
	}

	row, err := r.queries.UpsertArtifactSchema(ctx, sqlcgen.UpsertArtifactSchemaParams{
		SchemaRef:      normalized.Ref,
		Description:    normalized.Description,
		SchemaJson:     schemaJSON,
		SchemaDeclJson: declarationJSON,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return r.handleSchemaInsertErr(ctx, err, normalized, idempotencyKey)
	}

	return unmarshalArtifactSchema(row)
}

func (r *AgentOSArtifactSchemaCatalogRepo) handleSchemaInsertErr(ctx context.Context, insertErr error, normalized agentos.ArtifactSchema, idempotencyKey string) (agentos.ArtifactSchema, error) {
	// The conditional upsert reports a conflicting declaration by updating
	// nothing: no RETURNING row is the conflict, named for the caller.
	if errors.Is(insertErr, pgx.ErrNoRows) {
		return agentos.ArtifactSchema{}, fmt.Errorf("%w: artifact schema %q already exists with a different declaration", agentoscore.ErrInvalidArtifact, normalized.Ref)
	}

	if !isPostgresUniqueViolation(insertErr) {
		return agentos.ArtifactSchema{}, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - upsert: %w", insertErr)
	}

	existing, exists, lookupErr := r.schemaByIdempotencyKey(ctx, idempotencyKey)
	if lookupErr != nil {
		return agentos.ArtifactSchema{}, lookupErr
	}

	if !exists {
		return agentos.ArtifactSchema{}, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - upsert: %w", insertErr)
	}

	if err := agentosplan.ValidateArtifactSchemaRegistrationIdempotency(existing, normalized); err != nil {
		return agentos.ArtifactSchema{}, err
	}

	return existing, nil
}

func validateArtifactSchemaIdempotencyKey(normalized agentos.ArtifactSchema, idempotencyKey string) error {
	expectedKey, err := agentosplan.ArtifactSchemaRegistrationIdempotencyKey(normalized)
	if err != nil {
		return err
	}

	if idempotencyKey != expectedKey {
		return fmt.Errorf("%w: artifact schema registration idempotency key must match declaration", agentoscore.ErrInvalidArtifact)
	}

	return nil
}

// GetArtifactSchema loads the raw JSON Schema registered under the given schema reference.
func (r *AgentOSArtifactSchemaCatalogRepo) GetArtifactSchema(ctx context.Context, schemaRef string) (json.RawMessage, bool, error) {
	schemaJSON, err := r.queries.GetArtifactSchemaJSON(ctx, schemaRef)
	if missingRow(err) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - GetArtifactSchema - query: %w", err)
	}

	// The raw message is copied out of the binding's buffer so the caller
	// owns bytes no later query can reuse.
	return append(json.RawMessage(nil), schemaJSON...), true, nil
}

func (r *AgentOSArtifactSchemaCatalogRepo) schemaByRef(ctx context.Context, schemaRef string) (agentos.ArtifactSchema, bool, error) {
	declarationJSON, err := r.queries.GetArtifactSchemaByRef(ctx, schemaRef)
	if missingRow(err) {
		return agentos.ArtifactSchema{}, false, nil
	}

	if err != nil {
		return agentos.ArtifactSchema{}, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - schemaByRef - query: %w", err)
	}

	schema, err := unmarshalArtifactSchema(declarationJSON)
	if err != nil {
		return agentos.ArtifactSchema{}, false, err
	}

	return schema, true, nil
}

func (r *AgentOSArtifactSchemaCatalogRepo) schemaByIdempotencyKey(ctx context.Context, idempotencyKey string) (agentos.ArtifactSchema, bool, error) {
	declarationJSON, err := r.queries.GetArtifactSchemaByIdempotencyKey(ctx, idempotencyKey)
	if missingRow(err) {
		return agentos.ArtifactSchema{}, false, nil
	}

	if err != nil {
		return agentos.ArtifactSchema{}, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - schemaByIdempotencyKey - query: %w", err)
	}

	schema, err := unmarshalArtifactSchema(declarationJSON)
	if err != nil {
		return agentos.ArtifactSchema{}, false, err
	}

	return schema, true, nil
}

func unmarshalArtifactSchema(data []byte) (agentos.ArtifactSchema, error) {
	var schema agentos.ArtifactSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return agentos.ArtifactSchema{}, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - unmarshalArtifactSchema - decode: %w", err)
	}

	if err := agentosplan.ValidateArtifactSchema(schema); err != nil {
		return agentos.ArtifactSchema{}, err
	}

	return schema, nil
}

var (
	_ agentosplan.ArtifactSchemaCatalog  = (*AgentOSArtifactSchemaCatalogRepo)(nil)
	_ agentosplan.ArtifactSchemaRegistry = (*AgentOSArtifactSchemaCatalogRepo)(nil)
)
