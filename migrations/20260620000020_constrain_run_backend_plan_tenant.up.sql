ALTER TABLE plans
    ADD CONSTRAINT plans_tenant_plan_unique
    UNIQUE (account_id, project_id, plan_id);

ALTER TABLE run_backend_index
    ADD CONSTRAINT run_backend_index_plan_tenant_fk
    FOREIGN KEY (account_id, project_id, plan_id)
    REFERENCES plans(account_id, project_id, plan_id)
    ON DELETE CASCADE;
