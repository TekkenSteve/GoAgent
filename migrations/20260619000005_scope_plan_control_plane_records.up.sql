ALTER TABLE plan_events
    ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS project_id TEXT NOT NULL DEFAULT '';

UPDATE plan_events
SET account_id = plans.account_id,
    project_id = plans.project_id
FROM plans
WHERE plan_events.plan_id = plans.plan_id;

CREATE INDEX IF NOT EXISTS idx_plan_events_tenant_scope
    ON plan_events(account_id, project_id, plan_id, sequence);

ALTER TABLE artifacts
    ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS project_id TEXT NOT NULL DEFAULT '';

UPDATE artifacts
SET account_id = plans.account_id,
    project_id = plans.project_id
FROM plans
WHERE artifacts.plan_id = plans.plan_id;

CREATE INDEX IF NOT EXISTS idx_artifacts_tenant_scope
    ON artifacts(account_id, project_id, plan_id, node_id, run_id, created_at);

ALTER TABLE audit_logs
    ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS project_id TEXT NOT NULL DEFAULT '';

UPDATE audit_logs
SET account_id = plans.account_id,
    project_id = plans.project_id
FROM plans
WHERE audit_logs.plan_id = plans.plan_id;

CREATE INDEX IF NOT EXISTS idx_audit_logs_tenant_scope
    ON audit_logs(account_id, project_id, plan_id, created_at);

ALTER TABLE plan_commands
    ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS project_id TEXT NOT NULL DEFAULT '';

UPDATE plan_commands
SET account_id = plans.account_id,
    project_id = plans.project_id
FROM plans
WHERE plan_commands.plan_id = plans.plan_id;

CREATE INDEX IF NOT EXISTS idx_plan_commands_tenant_status
    ON plan_commands(account_id, project_id, status, updated_at);
