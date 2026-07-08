CREATE OR REPLACE FUNCTION enforce_plan_command_status_lifecycle()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'pending' THEN
            RAISE EXCEPTION 'plan command must be inserted as pending, got %', NEW.status
                USING ERRCODE = '23514';
        END IF;

        RETURN NEW;
    END IF;

    IF OLD.status = 'delivered' AND NEW.status <> 'delivered' THEN
        RAISE EXCEPTION 'delivered plan command is terminal'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.status <> 'pending' AND NEW.status = 'pending' THEN
        RAISE EXCEPTION 'plan command status cannot move back to pending'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS plan_commands_status_lifecycle ON plan_commands;

CREATE TRIGGER plan_commands_status_lifecycle
BEFORE INSERT OR UPDATE OF status ON plan_commands
FOR EACH ROW
EXECUTE FUNCTION enforce_plan_command_status_lifecycle();
