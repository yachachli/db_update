CREATE TABLE IF NOT EXISTS cfb_model_v1.identity_sources (
 id bigserial PRIMARY KEY, observed_at timestamptz NOT NULL, season integer NOT NULL,
 endpoint text NOT NULL, parameters jsonb NOT NULL, payload jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS cfb_model_v1.quote_identity_audits (
 id bigserial PRIMARY KEY, quote_id bigint NOT NULL REFERENCES cfb_model_v1.prop_quotes(id),
 audited_at timestamptz NOT NULL DEFAULT clock_timestamp(), status text NOT NULL,
 evidence jsonb NOT NULL, availability_status text NOT NULL DEFAULT 'unknown' CHECK(availability_status='unknown'),
 market_ready boolean NOT NULL DEFAULT false CHECK(NOT market_ready)
);
