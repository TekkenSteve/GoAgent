DROP INDEX IF EXISTS idx_artifacts_idempotency_key;

ALTER TABLE artifacts
    DROP COLUMN IF EXISTS idempotency_key;
