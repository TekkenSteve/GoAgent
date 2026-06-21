ALTER TABLE plan_events
    DROP CONSTRAINT IF EXISTS plan_events_transition_snapshot_pair;

ALTER TABLE plan_events
    DROP COLUMN IF EXISTS transition_snapshot_json,
    DROP COLUMN IF EXISTS transition_snapshot_digest;
