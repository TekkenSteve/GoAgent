CREATE TABLE IF NOT EXISTS processes (
    process_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    spec_json JSONB NOT NULL,
    status_json JSONB NOT NULL,
    event_sequence BIGINT NOT NULL DEFAULT 0,
    requested_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT processes_account_id_required CHECK (account_id <> ''),
    CONSTRAINT processes_project_id_required CHECK (project_id <> ''),
    CONSTRAINT processes_resource_identity_required CHECK (resource_kind <> '' AND resource_id <> ''),
    CONSTRAINT processes_idempotency_key_required CHECK (idempotency_key <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_processes_tenant_process
    ON processes(account_id, project_id, process_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_processes_idempotency_key
    ON processes(account_id, project_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_processes_resource
    ON processes(account_id, project_id, resource_kind, resource_id);
CREATE INDEX IF NOT EXISTS idx_processes_lifecycle
    ON processes(lifecycle_state);

CREATE TABLE IF NOT EXISTS process_events (
    event_id TEXT PRIMARY KEY,
    process_id TEXT NOT NULL REFERENCES processes(process_id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    event_json JSONB NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (process_id, sequence),
    CONSTRAINT process_events_account_id_required CHECK (account_id <> ''),
    CONSTRAINT process_events_project_id_required CHECK (project_id <> ''),
    CONSTRAINT process_events_resource_identity_required CHECK (resource_kind <> '' AND resource_id <> ''),
    CONSTRAINT process_events_idempotency_key_required CHECK (idempotency_key <> ''),
    CONSTRAINT process_events_process_tenant_fk FOREIGN KEY (account_id, project_id, process_id)
        REFERENCES processes(account_id, project_id, process_id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_process_events_idempotency_key
    ON process_events(account_id, project_id, process_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_process_events_scope
    ON process_events(account_id, project_id, process_id, sequence);
CREATE INDEX IF NOT EXISTS idx_process_events_type
    ON process_events(event_type);

CREATE TABLE IF NOT EXISTS process_status_updates (
    process_id TEXT NOT NULL REFERENCES processes(process_id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    status_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, project_id, process_id, idempotency_key),
    CONSTRAINT process_status_updates_idempotency_key_required CHECK (idempotency_key <> ''),
    CONSTRAINT process_status_updates_process_tenant_fk FOREIGN KEY (account_id, project_id, process_id)
        REFERENCES processes(account_id, project_id, process_id) ON DELETE RESTRICT
);
