-- Artifact JSON Schema declarations. Two documents live per row: the raw
-- JSON Schema (what validators fetch) and the full declaration (what
-- registration replays compare against).

-- UpsertArtifactSchema registers a declaration. The conditional DO UPDATE
-- means a conflicting declaration under the same schema_ref updates nothing
-- and returns no row — ErrNoRows to the caller, which turns it into an
-- invalid-artifact error naming the conflict.
-- name: UpsertArtifactSchema :one
INSERT INTO agentos_artifact_schemas (
    schema_ref, description, schema_json, schema_decl_json, idempotency_key
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (schema_ref) DO UPDATE SET
    description = EXCLUDED.description,
    schema_json = EXCLUDED.schema_json,
    schema_decl_json = EXCLUDED.schema_decl_json,
    idempotency_key = EXCLUDED.idempotency_key,
    updated_at = NOW()
WHERE agentos_artifact_schemas.idempotency_key = EXCLUDED.idempotency_key
RETURNING schema_decl_json;

-- GetArtifactSchemaJSON loads the raw JSON Schema registered under a
-- reference — what artifact validation fetches.
-- name: GetArtifactSchemaJSON :one
SELECT schema_json
FROM agentos_artifact_schemas
WHERE schema_ref = $1;

-- GetArtifactSchemaByRef loads the full declaration by schema reference, so
-- a replayed registration can compare declarations for equality.
-- name: GetArtifactSchemaByRef :one
SELECT schema_decl_json
FROM agentos_artifact_schemas
WHERE schema_ref = $1;

-- GetArtifactSchemaByIdempotencyKey loads a declaration by its registration
-- key.
-- name: GetArtifactSchemaByIdempotencyKey :one
SELECT schema_decl_json
FROM agentos_artifact_schemas
WHERE idempotency_key = $1;
