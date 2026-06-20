CREATE OR REPLACE FUNCTION protect_plan_identity()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'plan identity cannot be deleted'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.plan_id IS DISTINCT FROM NEW.plan_id
        OR OLD.thread_id IS DISTINCT FROM NEW.thread_id
        OR OLD.account_id IS DISTINCT FROM NEW.account_id
        OR OLD.project_id IS DISTINCT FROM NEW.project_id
        OR OLD.idempotency_key IS DISTINCT FROM NEW.idempotency_key
        OR OLD.requested_at IS DISTINCT FROM NEW.requested_at
        OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
        RAISE EXCEPTION 'plan identity cannot be updated'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.spec_json->'plan_id' IS DISTINCT FROM NEW.spec_json->'plan_id'
        OR OLD.spec_json->'thread_id' IS DISTINCT FROM NEW.spec_json->'thread_id'
        OR OLD.spec_json->'account_id' IS DISTINCT FROM NEW.spec_json->'account_id'
        OR OLD.spec_json->'project_id' IS DISTINCT FROM NEW.spec_json->'project_id'
        OR OLD.spec_json->'idempotency_key' IS DISTINCT FROM NEW.spec_json->'idempotency_key'
        OR OLD.spec_json->'requested_at' IS DISTINCT FROM NEW.spec_json->'requested_at'
        OR OLD.spec_json->'inputs' IS DISTINCT FROM NEW.spec_json->'inputs'
        OR OLD.spec_json->'metadata' IS DISTINCT FROM NEW.spec_json->'metadata'
        OR OLD.spec_json->'policy' IS DISTINCT FROM NEW.spec_json->'policy' THEN
        RAISE EXCEPTION 'plan spec identity cannot be updated'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS plan_identity_immutable ON plans;

CREATE TRIGGER plan_identity_immutable
BEFORE UPDATE OR DELETE ON plans
FOR EACH ROW
EXECUTE FUNCTION protect_plan_identity();

CREATE OR REPLACE FUNCTION protect_plan_node_identity()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'plan nodes cannot be deleted'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.plan_id IS DISTINCT FROM NEW.plan_id
        OR OLD.node_id IS DISTINCT FROM NEW.node_id
        OR OLD.backend_kind IS DISTINCT FROM NEW.backend_kind
        OR OLD.backend_name IS DISTINCT FROM NEW.backend_name
        OR OLD.capability IS DISTINCT FROM NEW.capability
        OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
        RAISE EXCEPTION 'plan node identity cannot be updated'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS plan_node_identity_immutable ON plan_nodes;

CREATE TRIGGER plan_node_identity_immutable
BEFORE UPDATE OR DELETE ON plan_nodes
FOR EACH ROW
EXECUTE FUNCTION protect_plan_node_identity();
