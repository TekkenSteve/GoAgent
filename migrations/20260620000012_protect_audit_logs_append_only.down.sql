DROP TRIGGER IF EXISTS audit_logs_append_only ON audit_logs;
DROP FUNCTION IF EXISTS protect_audit_logs_append_only();

CREATE OR REPLACE FUNCTION protect_delivered_plan_command_audit()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM plan_commands cmd
        WHERE cmd.status = 'delivered'
          AND cmd.plan_id = OLD.plan_id
          AND cmd.account_id = OLD.account_id
          AND cmd.project_id = OLD.project_id
          AND cmd.actor_id = OLD.actor_id
          AND cmd.action = OLD.action
          AND cmd.idempotency_key = OLD.idempotency_key
          AND cmd.payload_json = OLD.payload_json
    ) THEN
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'audit record for delivered plan command cannot be deleted'
                USING ERRCODE = '23514';
        END IF;

        RAISE EXCEPTION 'audit record for delivered plan command cannot be updated'
            USING ERRCODE = '23514';
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_logs_delivered_command_audit ON audit_logs;

CREATE TRIGGER audit_logs_delivered_command_audit
BEFORE UPDATE OR DELETE ON audit_logs
FOR EACH ROW
EXECUTE FUNCTION protect_delivered_plan_command_audit();
