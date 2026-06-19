ALTER TABLE artifacts
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_artifacts_idempotency_key
    ON artifacts(idempotency_key)
    WHERE idempotency_key <> '';
