ALTER TABLE audit_logs
    DROP CONSTRAINT IF EXISTS audit_logs_plan_node_run_fk,
    DROP CONSTRAINT IF EXISTS audit_logs_plan_node_fk,
    DROP CONSTRAINT IF EXISTS audit_logs_node_run_pair,
    DROP CONSTRAINT IF EXISTS audit_logs_plan_fk;

UPDATE audit_logs
SET node_id = ''
WHERE node_id IS NULL;

UPDATE audit_logs
SET run_id = ''
WHERE run_id IS NULL;

ALTER TABLE audit_logs
    ALTER COLUMN node_id SET NOT NULL,
    ALTER COLUMN run_id SET NOT NULL,
    ALTER COLUMN node_id SET DEFAULT '',
    ALTER COLUMN run_id SET DEFAULT '';
