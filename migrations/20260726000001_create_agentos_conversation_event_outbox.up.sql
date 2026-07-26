CREATE TABLE agentos_conversation_event_outbox (
    thread_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (thread_id, sequence),
    FOREIGN KEY (thread_id, sequence)
        REFERENCES agentos_conversation_events(thread_id, sequence)
        ON DELETE CASCADE
);

CREATE INDEX idx_agentos_conversation_event_outbox_ready
    ON agentos_conversation_event_outbox(available_at, created_at);
