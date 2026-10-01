CREATE TABLE IF NOT EXISTS cfb_model_v1.pipeline_runs (
 id bigserial PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz, mode text NOT NULL, season integer NOT NULL,
 status text NOT NULL CHECK(status IN ('running','success','failed')),
 current_stage text, details jsonb NOT NULL DEFAULT '{}'::jsonb, error_type text
);
