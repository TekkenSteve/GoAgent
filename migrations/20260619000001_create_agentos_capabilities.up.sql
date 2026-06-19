CREATE TABLE IF NOT EXISTS agentos_capabilities (
    backend_kind TEXT NOT NULL,
    backend_name TEXT NOT NULL,
    capability_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    capability_json JSONB NOT NULL,
    idempotency_key TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (backend_kind, backend_name, capability_name)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_agentos_capabilities_idempotency_key
    ON agentos_capabilities(idempotency_key)
    WHERE idempotency_key <> '';

CREATE INDEX IF NOT EXISTS idx_agentos_capabilities_backend
    ON agentos_capabilities(backend_kind, backend_name);
