ALTER TABLE artifacts
    ALTER COLUMN node_id DROP DEFAULT,
    ALTER COLUMN run_id DROP DEFAULT,
    ALTER COLUMN node_id DROP NOT NULL,
    ALTER COLUMN run_id DROP NOT NULL;

UPDATE artifacts
SET node_id = NULL
WHERE node_id = '';

UPDATE artifacts
SET run_id = NULL
WHERE run_id = '';

ALTER TABLE run_backend_index
    ADD CONSTRAINT run_backend_index_plan_node_run_unique
    UNIQUE (plan_id, node_id, run_id);

ALTER TABLE artifacts
    ADD CONSTRAINT artifacts_plan_fk
    FOREIGN KEY (plan_id)
    REFERENCES plans(plan_id)
    ON DELETE CASCADE,
    ADD CONSTRAINT artifacts_node_run_pair
    CHECK (
        (node_id IS NULL AND run_id IS NULL) OR
        (node_id IS NOT NULL AND run_id IS NOT NULL)
    ),
    ADD CONSTRAINT artifacts_plan_node_fk
    FOREIGN KEY (plan_id, node_id)
    REFERENCES plan_nodes(plan_id, node_id)
    ON DELETE CASCADE,
    ADD CONSTRAINT artifacts_plan_node_run_fk
    FOREIGN KEY (plan_id, node_id, run_id)
    REFERENCES run_backend_index(plan_id, node_id, run_id)
    ON DELETE CASCADE;
