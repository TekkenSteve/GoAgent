ALTER TABLE plans
    ADD CONSTRAINT plans_plan_id_required CHECK (plan_id <> ''),
    ADD CONSTRAINT plans_account_id_required CHECK (account_id <> ''),
    ADD CONSTRAINT plans_project_id_required CHECK (project_id <> ''),
    ADD CONSTRAINT plans_lifecycle_state_required CHECK (lifecycle_state <> '');

ALTER TABLE plan_nodes
    ADD CONSTRAINT plan_nodes_plan_id_required CHECK (plan_id <> ''),
    ADD CONSTRAINT plan_nodes_node_id_required CHECK (node_id <> ''),
    ADD CONSTRAINT plan_nodes_lifecycle_state_required CHECK (lifecycle_state <> ''),
    ADD CONSTRAINT plan_nodes_attempts_non_negative CHECK (attempts >= 0);

ALTER TABLE plan_events
    ADD CONSTRAINT plan_events_event_id_required CHECK (event_id <> ''),
    ADD CONSTRAINT plan_events_plan_id_required CHECK (plan_id <> ''),
    ADD CONSTRAINT plan_events_account_id_required CHECK (account_id <> ''),
    ADD CONSTRAINT plan_events_project_id_required CHECK (project_id <> ''),
    ADD CONSTRAINT plan_events_event_type_required CHECK (event_type <> ''),
    ADD CONSTRAINT plan_events_sequence_positive CHECK (sequence > 0);

ALTER TABLE run_backend_index
    ADD CONSTRAINT run_backend_index_run_id_required CHECK (run_id <> ''),
    ADD CONSTRAINT run_backend_index_account_id_required CHECK (account_id <> ''),
    ADD CONSTRAINT run_backend_index_project_id_required CHECK (project_id <> ''),
    ADD CONSTRAINT run_backend_index_backend_kind_required CHECK (backend_kind <> ''),
    ADD CONSTRAINT run_backend_index_backend_name_required CHECK (backend_name <> ''),
    ADD CONSTRAINT run_backend_index_lifecycle_state_required CHECK (lifecycle_state <> '');

ALTER TABLE artifacts
    ADD CONSTRAINT artifacts_artifact_id_required CHECK (artifact_id <> ''),
    ADD CONSTRAINT artifacts_plan_id_required CHECK (plan_id <> ''),
    ADD CONSTRAINT artifacts_account_id_required CHECK (account_id <> ''),
    ADD CONSTRAINT artifacts_project_id_required CHECK (project_id <> ''),
    ADD CONSTRAINT artifacts_name_required CHECK (name <> ''),
    ADD CONSTRAINT artifacts_kind_required CHECK (kind <> '');

ALTER TABLE audit_logs
    ADD CONSTRAINT audit_logs_audit_id_required CHECK (audit_id <> ''),
    ADD CONSTRAINT audit_logs_plan_id_required CHECK (plan_id <> ''),
    ADD CONSTRAINT audit_logs_account_id_required CHECK (account_id <> ''),
    ADD CONSTRAINT audit_logs_project_id_required CHECK (project_id <> ''),
    ADD CONSTRAINT audit_logs_action_required CHECK (action <> '');

ALTER TABLE plan_commands
    ADD CONSTRAINT plan_commands_command_id_required CHECK (command_id <> ''),
    ADD CONSTRAINT plan_commands_plan_id_required CHECK (plan_id <> ''),
    ADD CONSTRAINT plan_commands_account_id_required CHECK (account_id <> ''),
    ADD CONSTRAINT plan_commands_project_id_required CHECK (project_id <> ''),
    ADD CONSTRAINT plan_commands_action_required CHECK (action <> '');

ALTER TABLE plan_metric_checkpoints
    ADD CONSTRAINT plan_metric_checkpoints_plan_id_required CHECK (plan_id <> ''),
    ADD CONSTRAINT plan_metric_checkpoints_account_id_required CHECK (account_id <> ''),
    ADD CONSTRAINT plan_metric_checkpoints_project_id_required CHECK (project_id <> '');
