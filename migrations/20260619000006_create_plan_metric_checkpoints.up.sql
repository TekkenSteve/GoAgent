CREATE TABLE IF NOT EXISTS plan_metric_checkpoints (
    exporter_id TEXT NOT NULL,
    plan_id TEXT NOT NULL REFERENCES plans(plan_id) ON DELETE CASCADE,
    account_id TEXT NOT NULL DEFAULT '',
    project_id TEXT NOT NULL DEFAULT '',
    sequence BIGINT NOT NULL DEFAULT 0,
    projection_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exporter_id, plan_id),
    CONSTRAINT plan_metric_checkpoints_exporter_required CHECK (exporter_id <> ''),
    CONSTRAINT plan_metric_checkpoints_sequence_non_negative CHECK (sequence >= 0)
);

CREATE INDEX IF NOT EXISTS idx_plan_metric_checkpoints_tenant
    ON plan_metric_checkpoints(account_id, project_id, updated_at);
