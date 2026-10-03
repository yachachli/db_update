-- Existing databases need an ALTER, not a change to CREATE TABLE IF NOT EXISTS.
ALTER TABLE cfb_model_v1.pipeline_runs
    DROP CONSTRAINT IF EXISTS pipeline_runs_status_check;
ALTER TABLE cfb_model_v1.pipeline_runs
    ADD CONSTRAINT pipeline_runs_status_check
    CHECK (status IN ('running', 'success', 'failed', 'abandoned'));
