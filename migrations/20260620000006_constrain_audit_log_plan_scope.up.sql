ALTER TABLE audit_logs
    ALTER COLUMN node_id DROP DEFAULT,
    ALTER COLUMN run_id DROP DEFAULT,
    ALTER COLUMN node_id DROP NOT NULL,
    ALTER COLUMN run_id DROP NOT NULL;

UPDATE audit_logs
SET node_id = NULL
WHERE node_id = '';

UPDATE audit_logs
SET run_id = NULL
WHERE run_id = '';

ALTER TABLE audit_logs
    ADD CONSTRAINT audit_logs_plan_fk
    FOREIGN KEY (plan_id)
    REFERENCES plans(plan_id)
    ON DELETE CASCADE,
    ADD CONSTRAINT audit_logs_node_run_pair
    CHECK (
        (node_id IS NULL AND run_id IS NULL) OR
        (node_id IS NOT NULL AND run_id IS NOT NULL)
    ),
    ADD CONSTRAINT audit_logs_plan_node_fk
    FOREIGN KEY (plan_id, node_id)
    REFERENCES plan_nodes(plan_id, node_id)
    ON DELETE CASCADE,
    ADD CONSTRAINT audit_logs_plan_node_run_fk
    FOREIGN KEY (plan_id, node_id, run_id)
    REFERENCES run_backend_index(plan_id, node_id, run_id)
    ON DELETE CASCADE;
