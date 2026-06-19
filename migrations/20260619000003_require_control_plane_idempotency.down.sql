DROP INDEX IF EXISTS idx_agentos_capabilities_idempotency_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_agentos_capabilities_idempotency_key
    ON agentos_capabilities(idempotency_key)
    WHERE idempotency_key <> '';

DROP INDEX IF EXISTS idx_audit_logs_idempotency_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_logs_idempotency_key
    ON audit_logs(idempotency_key)
    WHERE idempotency_key <> '';

DROP INDEX IF EXISTS idx_artifacts_idempotency_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_artifacts_idempotency_key
    ON artifacts(idempotency_key)
    WHERE idempotency_key <> '';

DROP INDEX IF EXISTS idx_run_backend_index_idempotency_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_run_backend_index_idempotency_key
    ON run_backend_index(idempotency_key)
    WHERE idempotency_key <> '';

DROP INDEX IF EXISTS idx_plans_idempotency_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_plans_idempotency_key
    ON plans(idempotency_key)
    WHERE idempotency_key <> '';

ALTER TABLE agentos_capabilities
    DROP CONSTRAINT IF EXISTS agentos_capabilities_idempotency_key_required;

ALTER TABLE audit_logs
    DROP CONSTRAINT IF EXISTS audit_logs_idempotency_key_required;

ALTER TABLE artifacts
    DROP CONSTRAINT IF EXISTS artifacts_idempotency_key_required;

ALTER TABLE run_backend_index
    DROP CONSTRAINT IF EXISTS run_backend_index_idempotency_key_required;

ALTER TABLE plans
    DROP CONSTRAINT IF EXISTS plans_idempotency_key_required;
