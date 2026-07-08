CREATE OR REPLACE FUNCTION protect_plan_metric_samples_append_only()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'plan metric samples cannot be deleted'
            USING ERRCODE = '23514';
    END IF;

    RAISE EXCEPTION 'plan metric samples cannot be updated'
        USING ERRCODE = '23514';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS plan_metric_samples_append_only ON plan_metric_samples;

CREATE TRIGGER plan_metric_samples_append_only
BEFORE UPDATE OR DELETE ON plan_metric_samples
FOR EACH ROW
EXECUTE FUNCTION protect_plan_metric_samples_append_only();
