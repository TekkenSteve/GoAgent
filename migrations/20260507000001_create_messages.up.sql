CREATE TABLE IF NOT EXISTS messages(
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    tool_call_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_run_id ON messages(run_id);

CREATE TABLE IF NOT EXISTS tool_results(
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    tool_call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    result_json TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tool_results_run_id ON tool_results(run_id);

CREATE TABLE IF NOT EXISTS archives(
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    payload_type TEXT NOT NULL,
    content BYTEA NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_archives_run_id ON archives(run_id);
