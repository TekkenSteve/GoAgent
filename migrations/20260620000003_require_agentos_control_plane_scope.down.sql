ALTER TABLE plan_metric_checkpoints
    DROP CONSTRAINT IF EXISTS plan_metric_checkpoints_project_id_required,
    DROP CONSTRAINT IF EXISTS plan_metric_checkpoints_account_id_required,
    DROP CONSTRAINT IF EXISTS plan_metric_checkpoints_plan_id_required;

ALTER TABLE plan_commands
    DROP CONSTRAINT IF EXISTS plan_commands_action_required,
    DROP CONSTRAINT IF EXISTS plan_commands_project_id_required,
    DROP CONSTRAINT IF EXISTS plan_commands_account_id_required,
    DROP CONSTRAINT IF EXISTS plan_commands_plan_id_required,
    DROP CONSTRAINT IF EXISTS plan_commands_command_id_required;

ALTER TABLE audit_logs
    DROP CONSTRAINT IF EXISTS audit_logs_action_required,
    DROP CONSTRAINT IF EXISTS audit_logs_project_id_required,
    DROP CONSTRAINT IF EXISTS audit_logs_account_id_required,
    DROP CONSTRAINT IF EXISTS audit_logs_plan_id_required,
    DROP CONSTRAINT IF EXISTS audit_logs_audit_id_required;

ALTER TABLE artifacts
    DROP CONSTRAINT IF EXISTS artifacts_kind_required,
    DROP CONSTRAINT IF EXISTS artifacts_name_required,
    DROP CONSTRAINT IF EXISTS artifacts_project_id_required,
    DROP CONSTRAINT IF EXISTS artifacts_account_id_required,
    DROP CONSTRAINT IF EXISTS artifacts_plan_id_required,
    DROP CONSTRAINT IF EXISTS artifacts_artifact_id_required;

ALTER TABLE run_backend_index
    DROP CONSTRAINT IF EXISTS run_backend_index_lifecycle_state_required,
    DROP CONSTRAINT IF EXISTS run_backend_index_backend_name_required,
    DROP CONSTRAINT IF EXISTS run_backend_index_backend_kind_required,
    DROP CONSTRAINT IF EXISTS run_backend_index_project_id_required,
    DROP CONSTRAINT IF EXISTS run_backend_index_account_id_required,
    DROP CONSTRAINT IF EXISTS run_backend_index_run_id_required;

ALTER TABLE plan_events
    DROP CONSTRAINT IF EXISTS plan_events_sequence_positive,
    DROP CONSTRAINT IF EXISTS plan_events_event_type_required,
    DROP CONSTRAINT IF EXISTS plan_events_project_id_required,
    DROP CONSTRAINT IF EXISTS plan_events_account_id_required,
    DROP CONSTRAINT IF EXISTS plan_events_plan_id_required,
    DROP CONSTRAINT IF EXISTS plan_events_event_id_required;

ALTER TABLE plan_nodes
    DROP CONSTRAINT IF EXISTS plan_nodes_attempts_non_negative,
    DROP CONSTRAINT IF EXISTS plan_nodes_lifecycle_state_required,
    DROP CONSTRAINT IF EXISTS plan_nodes_node_id_required,
    DROP CONSTRAINT IF EXISTS plan_nodes_plan_id_required;

ALTER TABLE plans
    DROP CONSTRAINT IF EXISTS plans_lifecycle_state_required,
    DROP CONSTRAINT IF EXISTS plans_project_id_required,
    DROP CONSTRAINT IF EXISTS plans_account_id_required,
    DROP CONSTRAINT IF EXISTS plans_plan_id_required;
