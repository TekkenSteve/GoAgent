ALTER TABLE run_backend_index
    DROP CONSTRAINT IF EXISTS run_backend_index_plan_tenant_fk;

ALTER TABLE plans
    DROP CONSTRAINT IF EXISTS plans_tenant_plan_unique;
