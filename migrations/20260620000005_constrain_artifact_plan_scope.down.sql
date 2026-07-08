ALTER TABLE artifacts
    DROP CONSTRAINT IF EXISTS artifacts_plan_node_run_fk,
    DROP CONSTRAINT IF EXISTS artifacts_plan_node_fk,
    DROP CONSTRAINT IF EXISTS artifacts_node_run_pair,
    DROP CONSTRAINT IF EXISTS artifacts_plan_fk;

ALTER TABLE run_backend_index
    DROP CONSTRAINT IF EXISTS run_backend_index_plan_node_run_unique;

UPDATE artifacts
SET node_id = ''
WHERE node_id IS NULL;

UPDATE artifacts
SET run_id = ''
WHERE run_id IS NULL;

ALTER TABLE artifacts
    ALTER COLUMN node_id SET NOT NULL,
    ALTER COLUMN run_id SET NOT NULL,
    ALTER COLUMN node_id SET DEFAULT '',
    ALTER COLUMN run_id SET DEFAULT '';
