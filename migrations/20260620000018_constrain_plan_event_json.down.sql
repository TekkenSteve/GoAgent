ALTER TABLE plan_events
    DROP CONSTRAINT IF EXISTS plan_events_payload_json_matches_event,
    DROP CONSTRAINT IF EXISTS plan_events_event_json_identity_matches_columns;
