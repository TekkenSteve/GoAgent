CREATE OR REPLACE FUNCTION enforce_delivered_plan_command_audit()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'delivered' AND NOT EXISTS (
        SELECT 1
        FROM audit_logs audit
        WHERE audit.plan_id = NEW.plan_id
          AND audit.account_id = NEW.account_id
          AND audit.project_id = NEW.project_id
          AND audit.actor_id = NEW.actor_id
          AND audit.action = NEW.action
          AND audit.idempotency_key = NEW.idempotency_key
          AND audit.payload_json = NEW.payload_json
    ) THEN
        RAISE EXCEPTION 'delivered plan command requires matching audit record'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM plan_commands cmd
        WHERE cmd.status = 'delivered'
          AND NOT EXISTS (
              SELECT 1
              FROM audit_logs audit
              WHERE audit.plan_id = cmd.plan_id
                AND audit.account_id = cmd.account_id
                AND audit.project_id = cmd.project_id
                AND audit.actor_id = cmd.actor_id
                AND audit.action = cmd.action
                AND audit.idempotency_key = cmd.idempotency_key
                AND audit.payload_json = cmd.payload_json
          )
    ) THEN
        RAISE EXCEPTION 'delivered plan commands without matching audit records exist'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

DROP TRIGGER IF EXISTS plan_commands_delivered_audit ON plan_commands;

CREATE TRIGGER plan_commands_delivered_audit
BEFORE INSERT OR UPDATE OF
    status,
    plan_id,
    account_id,
    project_id,
    actor_id,
    action,
    idempotency_key,
    payload_json
ON plan_commands
FOR EACH ROW
EXECUTE FUNCTION enforce_delivered_plan_command_audit();
