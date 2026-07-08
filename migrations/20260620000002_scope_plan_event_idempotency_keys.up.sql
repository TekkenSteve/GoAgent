DROP INDEX IF EXISTS idx_plan_events_idempotency_key;
CREATE UNIQUE INDEX idx_plan_events_idempotency_key
    ON plan_events(account_id, project_id, plan_id, idempotency_key);
