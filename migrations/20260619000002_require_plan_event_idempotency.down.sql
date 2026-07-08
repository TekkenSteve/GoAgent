DROP INDEX IF EXISTS idx_plan_events_idempotency_key;

ALTER TABLE plan_events
    DROP CONSTRAINT IF EXISTS plan_events_idempotency_key_required;

CREATE UNIQUE INDEX IF NOT EXISTS idx_plan_events_idempotency_key
    ON plan_events(plan_id, idempotency_key)
    WHERE idempotency_key <> '';
