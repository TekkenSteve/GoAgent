package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AgentOSArtifactSchemaCatalogRepo persists artifact JSON Schema declarations.
type AgentOSArtifactSchemaCatalogRepo struct {
	*postgres.Postgres
}

// NewAgentOSArtifactSchemaCatalogRepo creates a Postgres-backed artifact schema catalog.
func NewAgentOSArtifactSchemaCatalogRepo(pg *postgres.Postgres) *AgentOSArtifactSchemaCatalogRepo {
	return &AgentOSArtifactSchemaCatalogRepo{Postgres: pg}
}

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

	registered, err := r.insertSchemaWithRetry(ctx, normalized, idempotencyKey)
	if err != nil {
		return agentos.ArtifactSchema{}, false, err
	}

	return registered, !exists, nil
}

func (r *AgentOSArtifactSchemaCatalogRepo) insertSchemaWithRetry(ctx context.Context, normalized agentos.ArtifactSchema, idempotencyKey string) (agentos.ArtifactSchema, error) {
	schemaJSON, err := json.Marshal(normalized.Schema)
	if err != nil {
		return agentos.ArtifactSchema{}, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - marshal schema: %w", err)
	}

	declarationJSON, errM := json.Marshal(normalized)
	if errM != nil {
		return agentos.ArtifactSchema{}, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - marshal declaration: %w", errM)
	}

	return r.execSchemaInsert(ctx, normalized, idempotencyKey, schemaJSON, declarationJSON)
}

func (r *AgentOSArtifactSchemaCatalogRepo) execSchemaInsert(ctx context.Context, normalized agentos.ArtifactSchema, idempotencyKey string, schemaJSON, declarationJSON []byte) (agentos.ArtifactSchema, error) {
	row := r.Pool.QueryRow(
		ctx, `
INSERT INTO agentos_artifact_schemas (
    schema_ref,
    description,
    schema_json,
    schema_decl_json,
    idempotency_key
) VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (schema_ref) DO UPDATE SET
    description = EXCLUDED.description,
    schema_json = EXCLUDED.schema_json,
    schema_decl_json = EXCLUDED.schema_decl_json,
    idempotency_key = EXCLUDED.idempotency_key,
    updated_at = NOW()
WHERE agentos_artifact_schemas.idempotency_key = EXCLUDED.idempotency_key
RETURNING schema_decl_json`,
		normalized.Ref,
		normalized.Description,
		schemaJSON,
		declarationJSON,
		idempotencyKey,
	)
	if err := row.Scan(&declarationJSON); err != nil {
		return r.handleSchemaInsertErr(ctx, err, normalized, idempotencyKey)
	}

	registered, err := unmarshalArtifactSchema(declarationJSON)
	if err != nil {
		return agentos.ArtifactSchema{}, err
	}

	return registered, nil
}

func (r *AgentOSArtifactSchemaCatalogRepo) handleSchemaInsertErr(ctx context.Context, scanErr error, normalized agentos.ArtifactSchema, idempotencyKey string) (agentos.ArtifactSchema, error) {
	if errors.Is(scanErr, pgx.ErrNoRows) {
		return agentos.ArtifactSchema{}, fmt.Errorf("%w: artifact schema %q already exists with a different declaration", agentos.ErrInvalidArtifact, normalized.Ref)
	}

	if !isPostgresUniqueViolation(scanErr) {
		return agentos.ArtifactSchema{}, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - upsert: %w", scanErr)
	}

	existing, exists, lookupErr := r.schemaByIdempotencyKey(ctx, idempotencyKey)
	if lookupErr != nil {
		return agentos.ArtifactSchema{}, lookupErr
	}

	if !exists {
		return agentos.ArtifactSchema{}, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - upsert: %w", scanErr)
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
		return fmt.Errorf("%w: artifact schema registration idempotency key must match declaration", agentos.ErrInvalidArtifact)
	}

	return nil
}

func (r *AgentOSArtifactSchemaCatalogRepo) GetArtifactSchema(ctx context.Context, schemaRef string) (json.RawMessage, bool, error) {
	sql, args, err := r.Builder.
		Select("schema_json").
		From("agentos_artifact_schemas").
		Where(sq.Eq{"schema_ref": schemaRef}).
		ToSql()
	if err != nil {
		return nil, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - GetArtifactSchema - builder: %w", err)
	}

	var schemaJSON []byte

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(&schemaJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - GetArtifactSchema - query: %w", err)
	}

	return append(json.RawMessage(nil), schemaJSON...), true, nil
}

func (r *AgentOSArtifactSchemaCatalogRepo) schemaByRef(ctx context.Context, schemaRef string) (agentos.ArtifactSchema, bool, error) {
	sql, args, err := r.Builder.
		Select("schema_decl_json").
		From("agentos_artifact_schemas").
		Where(sq.Eq{"schema_ref": schemaRef}).
		ToSql()
	if err != nil {
		return agentos.ArtifactSchema{}, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - schemaByRef - builder: %w", err)
	}

	return r.scanArtifactSchema(ctx, sql, args...)
}

func (r *AgentOSArtifactSchemaCatalogRepo) schemaByIdempotencyKey(ctx context.Context, idempotencyKey string) (agentos.ArtifactSchema, bool, error) {
	sql, args, err := r.Builder.
		Select("schema_decl_json").
		From("agentos_artifact_schemas").
		Where(sq.Eq{"idempotency_key": idempotencyKey}).
		ToSql()
	if err != nil {
		return agentos.ArtifactSchema{}, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - schemaByIdempotencyKey - builder: %w", err)
	}

	return r.scanArtifactSchema(ctx, sql, args...)
}

func scanJSONQueryRow(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (data []byte, found bool, err error) {
	err = pool.QueryRow(ctx, sql, args...).Scan(&data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, err
	}

	return data, true, nil
}

func (r *AgentOSArtifactSchemaCatalogRepo) scanArtifactSchema(ctx context.Context, sql string, args ...any) (agentos.ArtifactSchema, bool, error) {
	declarationJSON, found, err := scanJSONQueryRow(ctx, r.Pool, sql, args...)
	if err != nil {
		return agentos.ArtifactSchema{}, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - scanArtifactSchema - query: %w", err)
	}

	if !found {
		return agentos.ArtifactSchema{}, false, nil
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
