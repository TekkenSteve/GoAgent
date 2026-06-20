DROP TRIGGER IF EXISTS audit_logs_delivered_command_audit ON audit_logs;
DROP FUNCTION IF EXISTS protect_delivered_plan_command_audit();

CREATE OR REPLACE FUNCTION protect_audit_logs_append_only()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'audit logs cannot be deleted'
            USING ERRCODE = '23514';
    END IF;

    RAISE EXCEPTION 'audit logs cannot be updated'
        USING ERRCODE = '23514';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_logs_append_only ON audit_logs;

CREATE TRIGGER audit_logs_append_only
BEFORE UPDATE OR DELETE ON audit_logs
FOR EACH ROW
EXECUTE FUNCTION protect_audit_logs_append_only();
