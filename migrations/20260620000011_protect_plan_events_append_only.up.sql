CREATE OR REPLACE FUNCTION protect_plan_events_append_only()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'plan events cannot be deleted'
            USING ERRCODE = '23514';
    END IF;

    RAISE EXCEPTION 'plan events cannot be updated'
        USING ERRCODE = '23514';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS plan_events_append_only ON plan_events;

CREATE TRIGGER plan_events_append_only
BEFORE UPDATE OR DELETE ON plan_events
FOR EACH ROW
EXECUTE FUNCTION protect_plan_events_append_only();
