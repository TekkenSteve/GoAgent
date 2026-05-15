CREATE TABLE IF NOT EXISTS usage_records (
    id                BIGSERIAL       PRIMARY KEY,
    record_id         TEXT            NOT NULL UNIQUE,
    run_id            TEXT            NOT NULL,
    account_id        TEXT            NOT NULL,
    model_id          TEXT            NOT NULL,
    prompt_tokens     INT             NOT NULL DEFAULT 0,
    completion_tokens INT             NOT NULL DEFAULT 0,
    total_tokens      INT             NOT NULL DEFAULT 0,
    cost              NUMERIC(12,8)   NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_usage_records_account   ON usage_records(account_id);
CREATE INDEX idx_usage_records_run       ON usage_records(run_id);
CREATE INDEX idx_usage_records_created   ON usage_records(created_at DESC);
