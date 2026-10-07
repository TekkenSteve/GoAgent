-- Run leases and deferred projections.
--
-- Two failure modes share one mechanism, which is why they share one migration.
--
-- A run that is 'running' holds the thread's partial unique index, so it is the
-- thread's turn lock. If its terminal event never arrives — the backend died,
-- the pipeline dropped it — the thread is locked forever and every later turn
-- is refused. last_activity_at makes the lease observable: it advances with
-- every fact the run produces, so a run that has produced nothing for longer
-- than the lease is abandoned by a sweeper rather than believed forever.
ALTER TABLE agentos_conversation_runs
    ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- The sweeper only ever looks at running runs that have gone quiet, so the
-- index is partial: a deployment with thousands of finished runs stores a
-- handful of entries.
CREATE INDEX IF NOT EXISTS idx_agentos_conversation_runs_lease
    ON agentos_conversation_runs (last_activity_at)
    WHERE status = 'running';

-- A steer turn's run is admitted while its predecessor still streams, but its
-- RUN_STARTED cannot take the thread's turn until the predecessor finishes.
-- Rather than refusing the caller — which loses the turn, because the fact was
-- delivered once — the event is stored and its projection is queued here, and
-- the same sweeper applies it when the thread is free.
CREATE TABLE IF NOT EXISTS agentos_conversation_deferred_projections (
    thread_id  TEXT   NOT NULL REFERENCES agentos_threads(thread_id) ON DELETE CASCADE,
    sequence   BIGINT NOT NULL,
    run_id     TEXT   NOT NULL REFERENCES agentos_conversation_runs(run_id) ON DELETE CASCADE,
    event_type TEXT   NOT NULL,
    attempts   INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (thread_id, sequence),
    FOREIGN KEY (thread_id, sequence) REFERENCES agentos_conversation_events(thread_id, sequence) ON DELETE CASCADE
);

-- The sweeper walks deferred projections in the order the thread produced
-- them, so the primary key's order is the one it needs.
CREATE INDEX IF NOT EXISTS idx_agentos_conversation_deferred_run
    ON agentos_conversation_deferred_projections (run_id);
