CREATE TABLE IF NOT EXISTS plan_commands (
    command_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES plans(plan_id) ON DELETE CASCADE,
    actor_id TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    failure_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT plan_commands_idempotency_key_required CHECK (idempotency_key <> ''),
    CONSTRAINT plan_commands_status_valid CHECK (status IN ('pending', 'delivered', 'failed'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_plan_commands_idempotency_key
    ON plan_commands(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_plan_commands_plan ON plan_commands(plan_id, created_at);
CREATE INDEX IF NOT EXISTS idx_plan_commands_status ON plan_commands(status, updated_at);
