-- Restores the admission-time invariant. This fails when the thread holds a
-- pending run admitted behind a running one — roll this migration back only on
-- a database whose threads have at most one non-terminal run.
DROP INDEX IF EXISTS idx_agentos_conversation_runs_running;

CREATE UNIQUE INDEX idx_agentos_conversation_runs_open
    ON agentos_conversation_runs (thread_id)
    WHERE status IN ('pending', 'running');
