ALTER TABLE run_backend_index
    ALTER COLUMN plan_id DROP DEFAULT,
    ALTER COLUMN node_id DROP DEFAULT,
    ALTER COLUMN plan_id DROP NOT NULL,
    ALTER COLUMN node_id DROP NOT NULL;

UPDATE run_backend_index
SET plan_id = NULL
WHERE plan_id = '';

UPDATE run_backend_index
SET node_id = NULL
WHERE node_id = '';

ALTER TABLE run_backend_index
    ADD CONSTRAINT run_backend_index_plan_node_pair
    CHECK (
        (plan_id IS NULL AND node_id IS NULL) OR
        (plan_id IS NOT NULL AND node_id IS NOT NULL)
    ),
    ADD CONSTRAINT run_backend_index_plan_node_fk
    FOREIGN KEY (plan_id, node_id)
    REFERENCES plan_nodes(plan_id, node_id)
    ON DELETE CASCADE;
