-- Agent definitions and their version snapshots. Both tables were referenced
-- by the live AgentRepo without any migration ever creating them, so a fresh
-- database could not run the agent domain at all. CREATE TABLE IF NOT EXISTS
-- keeps this a no-op on databases where the tables already exist.

CREATE TABLE IF NOT EXISTS agents (
    agent_id        TEXT         PRIMARY KEY DEFAULT gen_random_uuid()::text,
    account_id      TEXT         NOT NULL,
    name            VARCHAR(255) NOT NULL,
    description     TEXT         NOT NULL DEFAULT '',
    system_prompt   TEXT         NOT NULL DEFAULT '',
    model_ref       VARCHAR(255) NOT NULL DEFAULT '',
    config          JSONB        NOT NULL DEFAULT '{}',
    current_version TEXT         NOT NULL DEFAULT '',
    is_default      BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agents_account_id ON agents(account_id);

CREATE TABLE IF NOT EXISTS agent_versions (
    version_id         TEXT         PRIMARY KEY,
    agent_id           TEXT         NOT NULL REFERENCES agents(agent_id) ON DELETE CASCADE,
    version_name       VARCHAR(255) NOT NULL DEFAULT '',
    system_prompt      TEXT         NOT NULL DEFAULT '',
    model_ref          VARCHAR(255) NOT NULL DEFAULT '',
    config             JSONB        NOT NULL DEFAULT '{}',
    tool_bindings      JSONB        NOT NULL DEFAULT '[]',
    change_description TEXT         NOT NULL DEFAULT '',
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_versions_agent_id ON agent_versions(agent_id);
