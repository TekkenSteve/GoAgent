CREATE TABLE IF NOT EXISTS agentos_artifact_schemas (
    schema_ref TEXT NOT NULL PRIMARY KEY,
    description TEXT NOT NULL DEFAULT '',
    schema_json JSONB NOT NULL,
    schema_decl_json JSONB NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agentos_artifact_schemas_idempotency_key_required CHECK (idempotency_key <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_agentos_artifact_schemas_idempotency_key
    ON agentos_artifact_schemas(idempotency_key);
