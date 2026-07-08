ALTER TABLE plans
    ADD CONSTRAINT plans_idempotency_key_required
    CHECK (idempotency_key <> '');

ALTER TABLE run_backend_index
    ADD CONSTRAINT run_backend_index_idempotency_key_required
    CHECK (idempotency_key <> '');

ALTER TABLE artifacts
    ADD CONSTRAINT artifacts_idempotency_key_required
    CHECK (idempotency_key <> '');

ALTER TABLE audit_logs
    ADD CONSTRAINT audit_logs_idempotency_key_required
    CHECK (idempotency_key <> '');

ALTER TABLE agentos_capabilities
    ADD CONSTRAINT agentos_capabilities_idempotency_key_required
    CHECK (idempotency_key <> '');

DROP INDEX IF EXISTS idx_plans_idempotency_key;
CREATE UNIQUE INDEX idx_plans_idempotency_key
    ON plans(idempotency_key);

DROP INDEX IF EXISTS idx_run_backend_index_idempotency_key;
CREATE UNIQUE INDEX idx_run_backend_index_idempotency_key
    ON run_backend_index(idempotency_key);

DROP INDEX IF EXISTS idx_artifacts_idempotency_key;
CREATE UNIQUE INDEX idx_artifacts_idempotency_key
    ON artifacts(idempotency_key);

DROP INDEX IF EXISTS idx_audit_logs_idempotency_key;
CREATE UNIQUE INDEX idx_audit_logs_idempotency_key
    ON audit_logs(idempotency_key);

DROP INDEX IF EXISTS idx_agentos_capabilities_idempotency_key;
CREATE UNIQUE INDEX idx_agentos_capabilities_idempotency_key
    ON agentos_capabilities(idempotency_key);
