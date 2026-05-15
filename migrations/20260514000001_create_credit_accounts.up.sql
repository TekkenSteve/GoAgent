CREATE TABLE IF NOT EXISTS credit_accounts (
    account_id  TEXT            PRIMARY KEY,
    balance     NUMERIC(12,4)   NOT NULL DEFAULT 0,
    currency    TEXT            NOT NULL DEFAULT 'USD',
    version     BIGINT          NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
