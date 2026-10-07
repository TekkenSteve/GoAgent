-- Tool-call idempotency: a repeated call answers with its first result instead
-- of running a side effect twice. The key is the tool call's own id, so a
-- platform retry of the enclosing activity carries the same key.
CREATE TABLE IF NOT EXISTS agentos_tool_idempotency (
    key          text        PRIMARY KEY,
    run_id       text        NOT NULL,
    tool_call_id text        NOT NULL,
    tool_name    text        NOT NULL,
    output       jsonb       NOT NULL,
    attempts     integer     NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Reads filter by age and the store prunes on write, so the age index is what
-- keeps both cheap.
CREATE INDEX IF NOT EXISTS idx_agentos_tool_idempotency_created_at
    ON agentos_tool_idempotency (created_at);
