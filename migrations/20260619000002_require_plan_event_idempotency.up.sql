ALTER TABLE plan_events
    ADD CONSTRAINT plan_events_idempotency_key_required
    CHECK (idempotency_key <> '');

DROP INDEX IF EXISTS idx_plan_events_idempotency_key;

CREATE UNIQUE INDEX idx_plan_events_idempotency_key
    ON plan_events(plan_id, idempotency_key);
