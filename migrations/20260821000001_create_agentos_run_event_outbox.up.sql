CREATE TABLE agentos_run_event_outbox (
    run_id TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, sequence),
    FOREIGN KEY (run_id, sequence)
        REFERENCES agentos_run_events(run_id, sequence)
        ON DELETE CASCADE
);

CREATE INDEX idx_agentos_run_event_outbox_ready
    ON agentos_run_event_outbox(available_at, created_at);
