DROP INDEX IF EXISTS idx_plan_commands_idempotency_key;
CREATE UNIQUE INDEX idx_plan_commands_idempotency_key
    ON plan_commands(idempotency_key);

DROP INDEX IF EXISTS idx_audit_logs_idempotency_key;
CREATE UNIQUE INDEX idx_audit_logs_idempotency_key
    ON audit_logs(idempotency_key);

DROP INDEX IF EXISTS idx_artifacts_idempotency_key;
CREATE UNIQUE INDEX idx_artifacts_idempotency_key
    ON artifacts(idempotency_key);

DROP INDEX IF EXISTS idx_run_backend_index_idempotency_key;
CREATE UNIQUE INDEX idx_run_backend_index_idempotency_key
    ON run_backend_index(idempotency_key);

DROP INDEX IF EXISTS idx_plans_idempotency_key;
CREATE UNIQUE INDEX idx_plans_idempotency_key
    ON plans(idempotency_key);
