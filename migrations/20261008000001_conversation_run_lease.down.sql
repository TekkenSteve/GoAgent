DROP TABLE IF EXISTS agentos_conversation_deferred_projections;

DROP INDEX IF EXISTS idx_agentos_conversation_runs_lease;

ALTER TABLE agentos_conversation_runs DROP COLUMN IF EXISTS last_activity_at;
