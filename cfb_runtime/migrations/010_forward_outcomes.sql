CREATE TABLE IF NOT EXISTS cfb_model_v1.forward_outcomes (
 id bigserial PRIMARY KEY,
 research_id bigint NOT NULL REFERENCES cfb_model_v1.forward_research(id),
 observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 status text NOT NULL,
 details jsonb NOT NULL,
 evidence_sha256 text NOT NULL,
 market_ready boolean NOT NULL DEFAULT false CHECK (NOT market_ready)
);
CREATE INDEX IF NOT EXISTS forward_outcomes_latest
 ON cfb_model_v1.forward_outcomes(research_id,id DESC);
