DROP INDEX IF EXISTS idx_audit_logs_chain;

ALTER TABLE audit_logs
    DROP COLUMN IF EXISTS row_hash,
    DROP COLUMN IF EXISTS prev_hash;
