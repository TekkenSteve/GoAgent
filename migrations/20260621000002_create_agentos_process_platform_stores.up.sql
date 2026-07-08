CREATE TABLE IF NOT EXISTS ledger_entries (
    entry_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    process_id TEXT NOT NULL DEFAULT '',
    resource_kind TEXT NOT NULL DEFAULT '',
    resource_id TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    idempotency_key TEXT NOT NULL,
    entry_json JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ledger_entries_account_id_required CHECK (account_id <> ''),
    CONSTRAINT ledger_entries_project_id_required CHECK (project_id <> ''),
    CONSTRAINT ledger_entries_kind_required CHECK (kind <> ''),
    CONSTRAINT ledger_entries_idempotency_key_required CHECK (idempotency_key <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_entries_idempotency_key
    ON ledger_entries(account_id, project_id, idempotency_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_entries_sequence
    ON ledger_entries(account_id, project_id, sequence);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_process
    ON ledger_entries(account_id, project_id, process_id, sequence);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_resource
    ON ledger_entries(account_id, project_id, resource_kind, resource_id, sequence);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_kind
    ON ledger_entries(account_id, project_id, kind, sequence);

CREATE TABLE IF NOT EXISTS governed_actions (
    action_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    process_id TEXT NOT NULL DEFAULT '',
    resource_kind TEXT NOT NULL DEFAULT '',
    resource_id TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    spec_json JSONB NOT NULL,
    status_json JSONB NOT NULL,
    requested_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT governed_actions_account_id_required CHECK (account_id <> ''),
    CONSTRAINT governed_actions_project_id_required CHECK (project_id <> ''),
    CONSTRAINT governed_actions_kind_required CHECK (kind <> ''),
    CONSTRAINT governed_actions_lifecycle_required CHECK (lifecycle_state <> ''),
    CONSTRAINT governed_actions_idempotency_key_required CHECK (idempotency_key <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_governed_actions_idempotency_key
    ON governed_actions(account_id, project_id, idempotency_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_governed_actions_tenant_action
    ON governed_actions(account_id, project_id, action_id);
CREATE INDEX IF NOT EXISTS idx_governed_actions_process
    ON governed_actions(account_id, project_id, process_id);
CREATE INDEX IF NOT EXISTS idx_governed_actions_resource
    ON governed_actions(account_id, project_id, resource_kind, resource_id);
CREATE INDEX IF NOT EXISTS idx_governed_actions_kind
    ON governed_actions(account_id, project_id, kind);
CREATE INDEX IF NOT EXISTS idx_governed_actions_lifecycle
    ON governed_actions(account_id, project_id, lifecycle_state);

CREATE TABLE IF NOT EXISTS governed_action_status_updates (
    action_id TEXT NOT NULL REFERENCES governed_actions(action_id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    status_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, project_id, action_id, idempotency_key),
    CONSTRAINT governed_action_status_updates_idempotency_key_required CHECK (idempotency_key <> ''),
    CONSTRAINT governed_action_status_updates_action_tenant_fk FOREIGN KEY (account_id, project_id, action_id)
        REFERENCES governed_actions(account_id, project_id, action_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS worksets (
    workset_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    process_id TEXT NOT NULL DEFAULT '',
    resource_kind TEXT NOT NULL DEFAULT '',
    resource_id TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    spec_json JSONB NOT NULL,
    status_json JSONB NOT NULL,
    requested_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT worksets_account_id_required CHECK (account_id <> ''),
    CONSTRAINT worksets_project_id_required CHECK (project_id <> ''),
    CONSTRAINT worksets_kind_required CHECK (kind <> ''),
    CONSTRAINT worksets_lifecycle_required CHECK (lifecycle_state <> ''),
    CONSTRAINT worksets_idempotency_key_required CHECK (idempotency_key <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_worksets_idempotency_key
    ON worksets(account_id, project_id, idempotency_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_worksets_tenant_workset
    ON worksets(account_id, project_id, workset_id);
CREATE INDEX IF NOT EXISTS idx_worksets_process
    ON worksets(account_id, project_id, process_id);
CREATE INDEX IF NOT EXISTS idx_worksets_resource
    ON worksets(account_id, project_id, resource_kind, resource_id);
CREATE INDEX IF NOT EXISTS idx_worksets_kind
    ON worksets(account_id, project_id, kind);
CREATE INDEX IF NOT EXISTS idx_worksets_lifecycle
    ON worksets(account_id, project_id, lifecycle_state);

CREATE TABLE IF NOT EXISTS workset_status_updates (
    workset_id TEXT NOT NULL REFERENCES worksets(workset_id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    status_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, project_id, workset_id, idempotency_key),
    CONSTRAINT workset_status_updates_idempotency_key_required CHECK (idempotency_key <> ''),
    CONSTRAINT workset_status_updates_workset_tenant_fk FOREIGN KEY (account_id, project_id, workset_id)
        REFERENCES worksets(account_id, project_id, workset_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS workset_chunk_results (
    workset_id TEXT NOT NULL REFERENCES worksets(workset_id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    chunk_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    result_json JSONB NOT NULL,
    status_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, project_id, workset_id, chunk_id, idempotency_key),
    CONSTRAINT workset_chunk_results_chunk_id_required CHECK (chunk_id <> ''),
    CONSTRAINT workset_chunk_results_idempotency_key_required CHECK (idempotency_key <> ''),
    CONSTRAINT workset_chunk_results_workset_tenant_fk FOREIGN KEY (account_id, project_id, workset_id)
        REFERENCES worksets(account_id, project_id, workset_id) ON DELETE RESTRICT
);
