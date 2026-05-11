ALTER TABLE workflow_triggers
ADD COLUMN template_vars_vals JSONB NOT NULL DEFAULT '{}';
