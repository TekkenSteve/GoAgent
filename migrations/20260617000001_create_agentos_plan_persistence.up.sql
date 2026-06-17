CREATE TABLE IF NOT EXISTS plans (
    plan_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL DEFAULT '',
    account_id TEXT NOT NULL DEFAULT '',
    project_id TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL DEFAULT '',
    lifecycle_state TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    spec_json JSONB NOT NULL,
    status_json JSONB NOT NULL,
    event_sequence BIGINT NOT NULL DEFAULT 0,
    requested_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_plans_idempotency_key
    ON plans(idempotency_key)
    WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS idx_plans_thread_id ON plans(thread_id);
CREATE INDEX IF NOT EXISTS idx_plans_account_project ON plans(account_id, project_id);
CREATE INDEX IF NOT EXISTS idx_plans_lifecycle ON plans(lifecycle_state);

CREATE TABLE IF NOT EXISTS plan_nodes (
    plan_id TEXT NOT NULL REFERENCES plans(plan_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    run_id TEXT NOT NULL DEFAULT '',
    backend_kind TEXT NOT NULL DEFAULT '',
    backend_name TEXT NOT NULL DEFAULT '',
    capability TEXT NOT NULL DEFAULT '',
    lifecycle_state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '',
    status_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (plan_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_plan_nodes_run_id ON plan_nodes(run_id);
CREATE INDEX IF NOT EXISTS idx_plan_nodes_lifecycle ON plan_nodes(lifecycle_state);

CREATE TABLE IF NOT EXISTS plan_events (
    event_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES plans(plan_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    idempotency_key TEXT NOT NULL DEFAULT '',
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    event_json JSONB NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (plan_id, sequence)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_plan_events_idempotency_key
    ON plan_events(plan_id, idempotency_key)
    WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS idx_plan_events_scope ON plan_events(plan_id, node_id, run_id, sequence);
CREATE INDEX IF NOT EXISTS idx_plan_events_type ON plan_events(event_type);

CREATE TABLE IF NOT EXISTS run_backend_index (
    run_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL DEFAULT '',
    node_id TEXT NOT NULL DEFAULT '',
    thread_id TEXT NOT NULL DEFAULT '',
    account_id TEXT NOT NULL DEFAULT '',
    project_id TEXT NOT NULL DEFAULT '',
    backend_kind TEXT NOT NULL,
    backend_name TEXT NOT NULL,
    idempotency_key TEXT NOT NULL DEFAULT '',
    lifecycle_state TEXT NOT NULL DEFAULT 'created',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_run_backend_index_idempotency_key
    ON run_backend_index(idempotency_key)
    WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS idx_run_backend_index_plan_node ON run_backend_index(plan_id, node_id);
CREATE INDEX IF NOT EXISTS idx_run_backend_index_backend ON run_backend_index(backend_kind, backend_name);
CREATE INDEX IF NOT EXISTS idx_run_backend_index_lifecycle ON run_backend_index(lifecycle_state);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL DEFAULT '',
    node_id TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    media_type TEXT NOT NULL DEFAULT '',
    uri TEXT NOT NULL DEFAULT '',
    size_bytes BIGINT NOT NULL DEFAULT 0,
    digest TEXT NOT NULL DEFAULT '',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_artifacts_plan_node ON artifacts(plan_id, node_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id);

CREATE TABLE IF NOT EXISTS audit_logs (
    audit_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL DEFAULT '',
    node_id TEXT NOT NULL DEFAULT '',
    actor_id TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,
    idempotency_key TEXT NOT NULL DEFAULT '',
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_logs_idempotency_key
    ON audit_logs(idempotency_key)
    WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS idx_audit_logs_plan ON audit_logs(plan_id, created_at);
