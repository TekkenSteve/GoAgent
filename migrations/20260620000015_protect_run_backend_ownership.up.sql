CREATE OR REPLACE FUNCTION protect_run_backend_ownership()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'run backend ownership cannot be deleted'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.run_id IS DISTINCT FROM NEW.run_id
        OR OLD.plan_id IS DISTINCT FROM NEW.plan_id
        OR OLD.node_id IS DISTINCT FROM NEW.node_id
        OR OLD.thread_id IS DISTINCT FROM NEW.thread_id
        OR OLD.account_id IS DISTINCT FROM NEW.account_id
        OR OLD.project_id IS DISTINCT FROM NEW.project_id
        OR OLD.backend_kind IS DISTINCT FROM NEW.backend_kind
        OR OLD.backend_name IS DISTINCT FROM NEW.backend_name
        OR OLD.idempotency_key IS DISTINCT FROM NEW.idempotency_key
        OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
        RAISE EXCEPTION 'run backend ownership identity cannot be updated'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS run_backend_ownership_immutable ON run_backend_index;

CREATE TRIGGER run_backend_ownership_immutable
BEFORE UPDATE OR DELETE ON run_backend_index
FOR EACH ROW
EXECUTE FUNCTION protect_run_backend_ownership();
