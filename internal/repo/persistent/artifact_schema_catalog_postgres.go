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
	expectedKey, err := agentosplan.ArtifactSchemaRegistrationIdempotencyKey(normalized)
	if err != nil {
		return agentos.ArtifactSchema{}, false, err
	}
	if idempotencyKey != expectedKey {
		return agentos.ArtifactSchema{}, false, fmt.Errorf("%w: artifact schema registration idempotency key must match declaration", agentos.ErrInvalidArtifact)
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

	schemaJSON, err := json.Marshal(normalized.Schema)
	if err != nil {
		return agentos.ArtifactSchema{}, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - marshal schema: %w", err)
	}
	declarationJSON, err := json.Marshal(normalized)
	if err != nil {
		return agentos.ArtifactSchema{}, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - marshal declaration: %w", err)
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
	created := !exists

	row := r.Pool.QueryRow(ctx, `
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
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.ArtifactSchema{}, false, fmt.Errorf("%w: artifact schema %q already exists with a different declaration", agentos.ErrInvalidArtifact, schema.Ref)
		}
		if isPostgresUniqueViolation(err) {
			existing, exists, lookupErr := r.schemaByIdempotencyKey(ctx, idempotencyKey)
			if lookupErr != nil {
				return agentos.ArtifactSchema{}, false, lookupErr
			}
			if exists {
				if err := agentosplan.ValidateArtifactSchemaRegistrationIdempotency(existing, normalized); err != nil {
					return agentos.ArtifactSchema{}, false, err
				}

				return existing, false, nil
			}
		}

		return agentos.ArtifactSchema{}, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - RegisterArtifactSchema - upsert: %w", err)
	}

	registered, err := unmarshalArtifactSchema(declarationJSON)
	if err != nil {
		return agentos.ArtifactSchema{}, false, err
	}

	return registered, created, nil
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

func (r *AgentOSArtifactSchemaCatalogRepo) scanArtifactSchema(ctx context.Context, sql string, args ...any) (agentos.ArtifactSchema, bool, error) {
	var declarationJSON []byte
	err := r.Pool.QueryRow(ctx, sql, args...).Scan(&declarationJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.ArtifactSchema{}, false, nil
		}

		return agentos.ArtifactSchema{}, false, fmt.Errorf("AgentOSArtifactSchemaCatalogRepo - scanArtifactSchema - query: %w", err)
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
