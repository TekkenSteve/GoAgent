DROP INDEX IF EXISTS idx_plan_commands_tenant_status;
ALTER TABLE plan_commands
    DROP COLUMN IF EXISTS project_id,
    DROP COLUMN IF EXISTS account_id;

DROP INDEX IF EXISTS idx_audit_logs_tenant_scope;
ALTER TABLE audit_logs
    DROP COLUMN IF EXISTS project_id,
    DROP COLUMN IF EXISTS account_id;

DROP INDEX IF EXISTS idx_artifacts_tenant_scope;
ALTER TABLE artifacts
    DROP COLUMN IF EXISTS project_id,
    DROP COLUMN IF EXISTS account_id;

DROP INDEX IF EXISTS idx_plan_events_tenant_scope;
ALTER TABLE plan_events
    DROP COLUMN IF EXISTS project_id,
    DROP COLUMN IF EXISTS account_id;
