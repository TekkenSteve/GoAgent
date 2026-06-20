CREATE OR REPLACE FUNCTION protect_plan_command_identity()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'plan commands cannot be deleted'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.command_id IS DISTINCT FROM NEW.command_id
        OR OLD.plan_id IS DISTINCT FROM NEW.plan_id
        OR OLD.account_id IS DISTINCT FROM NEW.account_id
        OR OLD.project_id IS DISTINCT FROM NEW.project_id
        OR OLD.actor_id IS DISTINCT FROM NEW.actor_id
        OR OLD.action IS DISTINCT FROM NEW.action
        OR OLD.idempotency_key IS DISTINCT FROM NEW.idempotency_key
        OR OLD.payload_json IS DISTINCT FROM NEW.payload_json
        OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
        RAISE EXCEPTION 'plan command identity cannot be updated'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.status = 'delivered'
        AND OLD.failure_reason IS DISTINCT FROM NEW.failure_reason THEN
        RAISE EXCEPTION 'delivered plan command failure reason is terminal'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.status = NEW.status
        AND NEW.status <> 'failed'
        AND OLD.failure_reason IS DISTINCT FROM NEW.failure_reason THEN
        RAISE EXCEPTION 'plan command failure reason can only change with a failed lifecycle'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS plan_commands_identity_immutable ON plan_commands;

CREATE TRIGGER plan_commands_identity_immutable
BEFORE UPDATE OR DELETE ON plan_commands
FOR EACH ROW
EXECUTE FUNCTION protect_plan_command_identity();
