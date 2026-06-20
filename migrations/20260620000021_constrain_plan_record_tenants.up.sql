ALTER TABLE plan_events
    ADD CONSTRAINT plan_events_plan_tenant_fk
    FOREIGN KEY (account_id, project_id, plan_id)
    REFERENCES plans(account_id, project_id, plan_id)
    ON DELETE CASCADE;

ALTER TABLE artifacts
    ADD CONSTRAINT artifacts_plan_tenant_fk
    FOREIGN KEY (account_id, project_id, plan_id)
    REFERENCES plans(account_id, project_id, plan_id)
    ON DELETE CASCADE;

ALTER TABLE audit_logs
    ADD CONSTRAINT audit_logs_plan_tenant_fk
    FOREIGN KEY (account_id, project_id, plan_id)
    REFERENCES plans(account_id, project_id, plan_id)
    ON DELETE CASCADE;

ALTER TABLE plan_commands
    ADD CONSTRAINT plan_commands_plan_tenant_fk
    FOREIGN KEY (account_id, project_id, plan_id)
    REFERENCES plans(account_id, project_id, plan_id)
    ON DELETE CASCADE;

ALTER TABLE plan_metric_checkpoints
    ADD CONSTRAINT plan_metric_checkpoints_plan_tenant_fk
    FOREIGN KEY (account_id, project_id, plan_id)
    REFERENCES plans(account_id, project_id, plan_id)
    ON DELETE CASCADE;

ALTER TABLE plan_metric_samples
    ADD CONSTRAINT plan_metric_samples_plan_tenant_fk
    FOREIGN KEY (account_id, project_id, plan_id)
    REFERENCES plans(account_id, project_id, plan_id)
    ON DELETE CASCADE;
