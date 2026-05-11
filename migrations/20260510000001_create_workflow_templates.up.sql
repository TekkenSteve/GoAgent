CREATE TABLE IF NOT EXISTS workflow_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    team_spec JSONB NOT NULL DEFAULT '{}',
    system_prompt TEXT NOT NULL DEFAULT '',
    default_model VARCHAR(255) NOT NULL DEFAULT '',
    tags TEXT[] NOT NULL DEFAULT '{}',
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_workflow_templates_account_id ON workflow_templates(account_id);
CREATE INDEX idx_workflow_templates_tags ON workflow_templates USING GIN(tags);
