CREATE TABLE IF NOT EXISTS credit_ledger (
    id          BIGSERIAL       PRIMARY KEY,
    account_id  TEXT            NOT NULL REFERENCES credit_accounts(account_id),
    amount      NUMERIC(12,4)   NOT NULL,
    type        TEXT            NOT NULL,
    description TEXT            NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_credit_ledger_account ON credit_ledger(account_id);
CREATE INDEX idx_credit_ledger_created  ON credit_ledger(created_at DESC);
