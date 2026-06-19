CREATE TABLE IF NOT EXISTS plan_metric_samples (
    metric_name TEXT NOT NULL,
    plan_id TEXT NOT NULL REFERENCES plans(plan_id) ON DELETE CASCADE,
    account_id TEXT NOT NULL DEFAULT '',
    project_id TEXT NOT NULL DEFAULT '',
    node_id TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL DEFAULT '',
    event_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit TEXT NOT NULL,
    labels_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    sample_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_name, plan_id, node_id, run_id, event_id, sequence),
    CONSTRAINT plan_metric_samples_metric_required CHECK (metric_name <> ''),
    CONSTRAINT plan_metric_samples_plan_required CHECK (plan_id <> ''),
    CONSTRAINT plan_metric_samples_account_required CHECK (account_id <> ''),
    CONSTRAINT plan_metric_samples_project_required CHECK (project_id <> ''),
    CONSTRAINT plan_metric_samples_event_required CHECK (event_id <> ''),
    CONSTRAINT plan_metric_samples_sequence_positive CHECK (sequence > 0),
    CONSTRAINT plan_metric_samples_unit_required CHECK (unit <> '')
);

CREATE INDEX IF NOT EXISTS idx_plan_metric_samples_tenant_time
    ON plan_metric_samples(account_id, project_id, sample_timestamp);

CREATE INDEX IF NOT EXISTS idx_plan_metric_samples_name_time
    ON plan_metric_samples(metric_name, sample_timestamp);
