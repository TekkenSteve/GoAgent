CREATE TABLE IF NOT EXISTS workflow_triggers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_id UUID NOT NULL REFERENCES workflow_templates(id) ON DELETE CASCADE,
    account_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    trigger_type VARCHAR(20) NOT NULL CHECK (trigger_type IN ('schedule', 'event')),
    cron_expression VARCHAR(100) NOT NULL DEFAULT '',
    event_slug VARCHAR(255) NOT NULL DEFAULT '',
    agent_prompt TEXT NOT NULL DEFAULT '',
    config JSONB NOT NULL DEFAULT '{}',
    template_vars TEXT[] NOT NULL DEFAULT '{}',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_fired_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_workflow_triggers_account_id ON workflow_triggers(account_id);
CREATE INDEX idx_workflow_triggers_template_id ON workflow_triggers(template_id);
CREATE INDEX idx_workflow_triggers_type ON workflow_triggers(trigger_type);
CREATE INDEX idx_workflow_triggers_event_slug ON workflow_triggers(event_slug) WHERE trigger_type = 'event';
