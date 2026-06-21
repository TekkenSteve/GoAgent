ALTER TABLE plans
    ADD CONSTRAINT plans_spec_json_identity_matches_columns
    CHECK (
        spec_json ? 'plan_id'
        AND spec_json ? 'account_id'
        AND spec_json ? 'project_id'
        AND spec_json ? 'idempotency_key'
        AND spec_json->>'plan_id' = plan_id
        AND COALESCE(spec_json->>'thread_id', '') = thread_id
        AND spec_json->>'account_id' = account_id
        AND spec_json->>'project_id' = project_id
        AND spec_json->>'idempotency_key' = idempotency_key
        AND (
            (requested_at IS NULL AND (
                NOT (spec_json ? 'requested_at')
                OR spec_json->>'requested_at' = '0001-01-01T00:00:00Z'
            ))
            OR
            (requested_at IS NOT NULL
                AND spec_json ? 'requested_at'
                AND (spec_json->>'requested_at')::timestamptz = requested_at
            )
        )
    ),
    ADD CONSTRAINT plans_status_json_matches_columns
    CHECK (
        status_json ? 'plan_id'
        AND status_json ? 'lifecycle_state'
        AND status_json->>'plan_id' = plan_id
        AND status_json->>'lifecycle_state' = lifecycle_state
        AND COALESCE(status_json->>'reason', '') = reason
    );

ALTER TABLE plan_nodes
    ADD CONSTRAINT plan_nodes_status_json_matches_columns
    CHECK (
        status_json ? 'node_id'
        AND status_json ? 'backend'
        AND status_json ? 'lifecycle_state'
        AND status_json->>'node_id' = node_id
        AND COALESCE(status_json->>'run_id', '') = run_id
        AND status_json->'backend'->>'kind' = backend_kind
        AND status_json->'backend'->>'name' = backend_name
        AND status_json->>'lifecycle_state' = lifecycle_state
        AND COALESCE((status_json->>'attempts')::integer, 0) = attempts
        AND COALESCE(status_json->>'reason', '') = reason
    );
