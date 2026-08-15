CREATE TABLE agentos_run_events (
    run_id TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL CHECK (event_type <> ''),
    thread_id TEXT NOT NULL DEFAULT '',
    process_id TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, sequence)
);

CREATE INDEX idx_agentos_run_events_thread ON agentos_run_events(thread_id, sequence);

CREATE OR REPLACE FUNCTION protect_agentos_run_events_append_only()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'agentos run events cannot be deleted'
            USING ERRCODE = '23514';
    END IF;

    RAISE EXCEPTION 'agentos run events cannot be updated'
        USING ERRCODE = '23514';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS agentos_run_events_append_only ON agentos_run_events;

CREATE TRIGGER agentos_run_events_append_only
BEFORE UPDATE OR DELETE ON agentos_run_events
FOR EACH ROW
EXECUTE FUNCTION protect_agentos_run_events_append_only();
