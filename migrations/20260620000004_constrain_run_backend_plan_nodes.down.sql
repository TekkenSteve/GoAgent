ALTER TABLE run_backend_index
    DROP CONSTRAINT IF EXISTS run_backend_index_plan_node_fk,
    DROP CONSTRAINT IF EXISTS run_backend_index_plan_node_pair;

UPDATE run_backend_index
SET plan_id = ''
WHERE plan_id IS NULL;

UPDATE run_backend_index
SET node_id = ''
WHERE node_id IS NULL;

ALTER TABLE run_backend_index
    ALTER COLUMN plan_id SET NOT NULL,
    ALTER COLUMN node_id SET NOT NULL,
    ALTER COLUMN plan_id SET DEFAULT '',
    ALTER COLUMN node_id SET DEFAULT '';
