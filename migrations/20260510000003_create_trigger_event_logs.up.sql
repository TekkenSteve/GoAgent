CREATE TABLE IF NOT EXISTS trigger_event_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trigger_id UUID NOT NULL REFERENCES workflow_triggers(id) ON DELETE CASCADE,
    template_id UUID NOT NULL,
    trigger_type VARCHAR(20) NOT NULL,
    success BOOLEAN NOT NULL DEFAULT TRUE,
    message TEXT NOT NULL DEFAULT '',
    fired_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_trigger_event_logs_trigger_id ON trigger_event_logs(trigger_id);
CREATE INDEX idx_trigger_event_logs_fired_at ON trigger_event_logs(fired_at);
CREATE INDEX idx_trigger_event_logs_success ON trigger_event_logs(success);
