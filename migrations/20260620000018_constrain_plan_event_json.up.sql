ALTER TABLE plan_events
    ADD CONSTRAINT plan_events_event_json_identity_matches_columns
    CHECK (
        event_json ? 'event_id'
        AND event_json ? 'event_type'
        AND event_json ? 'plan_id'
        AND event_json ? 'account_id'
        AND event_json ? 'project_id'
        AND event_json ? 'sequence'
        AND event_json ? 'timestamp'
        AND event_json->>'event_id' = event_id
        AND event_json->>'event_type' = event_type
        AND event_json->>'plan_id' = plan_id
        AND event_json->>'account_id' = account_id
        AND event_json->>'project_id' = project_id
        AND COALESCE(event_json->>'node_id', '') = node_id
        AND COALESCE(event_json->>'run_id', '') = run_id
        AND (event_json->>'sequence')::bigint = sequence
        AND (event_json->>'timestamp')::timestamptz = timestamp
    ),
    ADD CONSTRAINT plan_events_payload_json_matches_event
    CHECK (COALESCE(event_json->'payload', '{}'::jsonb) = payload_json);
