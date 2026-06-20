ALTER TABLE plan_nodes
    DROP CONSTRAINT IF EXISTS plan_nodes_status_json_matches_columns;

ALTER TABLE plans
    DROP CONSTRAINT IF EXISTS plans_status_json_matches_columns,
    DROP CONSTRAINT IF EXISTS plans_spec_json_identity_matches_columns;
