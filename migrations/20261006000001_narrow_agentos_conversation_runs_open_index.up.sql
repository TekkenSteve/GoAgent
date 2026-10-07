-- The one-active-run invariant is about concurrent streams, not admissions.
-- A steer or interrupt turn is admitted while the previous run is still on the
-- wire: its row lands as 'pending' and only starts once the running run reaches
-- a terminal state. Narrowing the partial index to 'running' makes that handoff
-- legal while still refusing two runs that would stream at the same time — the
-- refusal now happens at the RUN_STARTED transition, where the collision would
-- actually occur, instead of at insert time where it only meant "the thread is
-- busy" and rejected valid steering.
DROP INDEX IF EXISTS idx_agentos_conversation_runs_open;

CREATE UNIQUE INDEX idx_agentos_conversation_runs_running
    ON agentos_conversation_runs (thread_id)
    WHERE status = 'running';
