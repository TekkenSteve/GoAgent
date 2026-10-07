-- Audit hash chain.
--
-- An audit row that can be edited by whoever owns the database is not evidence:
-- the same account writes the rows and could rewrite them. Chaining each row to
-- the one before it makes an edit visible — the row's stored hash no longer
-- matches its contents, and every later row no longer matches its predecessor.
--
-- The chain is per tenant (account, project): that is the boundary the platform
-- already uses for run identity, and a per-tenant chain lets two tenants write
-- audits concurrently without serializing on one global tail.
--
-- Hashes are computed by the application, not by SQL, because the canonical
-- form of a row includes its JSON payload: PostgreSQL's jsonb text form and
-- Go's encoding differ in whitespace and key order, and a verifier that hashes
-- a differently-normalized payload would report every row as tampered.
ALTER TABLE audit_logs
    ADD COLUMN IF NOT EXISTS prev_hash TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS row_hash TEXT NOT NULL DEFAULT '';

-- Rows written before this migration carry no hash. They are reported as an
-- unchained prefix rather than presented as verified: a chain that silently
-- covered rows it never hashed would be worse than an honest gap.
CREATE INDEX IF NOT EXISTS idx_audit_logs_chain
    ON audit_logs (account_id, project_id, created_at, audit_id);
