CREATE TABLE agentos_threads (
    thread_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    next_sequence BIGINT NOT NULL DEFAULT 0 CHECK (next_sequence >= 0),
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agentos_threads_tenant ON agentos_threads(account_id, project_id, updated_at DESC);

CREATE TABLE agentos_conversation_runs (
    run_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES agentos_threads(thread_id) ON DELETE CASCADE,
    process_id TEXT NOT NULL DEFAULT '',
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'interrupted', 'cancelled', 'error')),
    outcome TEXT NOT NULL DEFAULT '',
    interrupt JSONB,
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL,
    resume_interrupt_id TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL DEFAULT '{}',
    last_source_sequence BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    UNIQUE (thread_id, idempotency_key)
);

CREATE INDEX idx_agentos_conversation_runs_thread ON agentos_conversation_runs(thread_id, created_at, run_id);
CREATE UNIQUE INDEX idx_agentos_conversation_runs_open ON agentos_conversation_runs(thread_id) WHERE status IN ('pending', 'running');

CREATE TABLE agentos_messages (
    message_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES agentos_threads(thread_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES agentos_conversation_runs(run_id) ON DELETE CASCADE,
    process_id TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('streaming', 'completed', 'error')),
    attachments JSONB NOT NULL DEFAULT '[]',
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_agentos_messages_thread ON agentos_messages(thread_id, created_at, message_id);
CREATE INDEX idx_agentos_messages_run ON agentos_messages(run_id, created_at, message_id);

CREATE TABLE agentos_conversation_events (
    thread_id TEXT NOT NULL REFERENCES agentos_threads(thread_id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    event_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL REFERENCES agentos_conversation_runs(run_id) ON DELETE CASCADE,
    process_id TEXT NOT NULL DEFAULT '',
    source_event_id TEXT NOT NULL DEFAULT '',
    source_sequence BIGINT NOT NULL DEFAULT 0,
    event_type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (thread_id, sequence)
);

CREATE UNIQUE INDEX idx_agentos_conversation_events_source
    ON agentos_conversation_events(thread_id, source_event_id)
    WHERE source_event_id <> '';
CREATE INDEX idx_agentos_conversation_events_run ON agentos_conversation_events(run_id, sequence);
