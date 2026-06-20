ALTER TABLE plan_metric_samples
    DROP CONSTRAINT IF EXISTS plan_metric_samples_plan_tenant_fk;

ALTER TABLE plan_metric_checkpoints
    DROP CONSTRAINT IF EXISTS plan_metric_checkpoints_plan_tenant_fk;

ALTER TABLE plan_commands
    DROP CONSTRAINT IF EXISTS plan_commands_plan_tenant_fk;

ALTER TABLE audit_logs
    DROP CONSTRAINT IF EXISTS audit_logs_plan_tenant_fk;

ALTER TABLE artifacts
    DROP CONSTRAINT IF EXISTS artifacts_plan_tenant_fk;

ALTER TABLE plan_events
    DROP CONSTRAINT IF EXISTS plan_events_plan_tenant_fk;
