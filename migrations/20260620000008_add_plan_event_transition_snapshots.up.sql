ALTER TABLE plan_events
    ADD COLUMN IF NOT EXISTS transition_snapshot_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS transition_snapshot_json JSONB;

ALTER TABLE plan_events
    ADD CONSTRAINT plan_events_transition_snapshot_pair
    CHECK (
        (transition_snapshot_digest = '' AND transition_snapshot_json IS NULL)
        OR
        (transition_snapshot_digest <> '' AND transition_snapshot_json IS NOT NULL)
    );
