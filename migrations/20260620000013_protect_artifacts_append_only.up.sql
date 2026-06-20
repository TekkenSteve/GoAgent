CREATE OR REPLACE FUNCTION protect_artifacts_append_only()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'artifacts cannot be deleted'
            USING ERRCODE = '23514';
    END IF;

    RAISE EXCEPTION 'artifacts cannot be updated'
        USING ERRCODE = '23514';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS artifacts_append_only ON artifacts;

CREATE TRIGGER artifacts_append_only
BEFORE UPDATE OR DELETE ON artifacts
FOR EACH ROW
EXECUTE FUNCTION protect_artifacts_append_only();
