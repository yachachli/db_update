CREATE TABLE IF NOT EXISTS cfb_model_v1.prop_captures (
 id bigserial PRIMARY KEY, captured_at timestamptz NOT NULL, event_id text NOT NULL,
 kickoff timestamptz NOT NULL, payload jsonb NOT NULL, game_id bigint REFERENCES cfb_model_v1.games(game_id),
 match_status text NOT NULL, market_ready boolean NOT NULL DEFAULT false CHECK (NOT market_ready)
);
CREATE TABLE IF NOT EXISTS cfb_model_v1.prop_quotes (
 id bigserial PRIMARY KEY, capture_id bigint NOT NULL REFERENCES cfb_model_v1.prop_captures(id),
 bookmaker text NOT NULL, market text NOT NULL, player_name text NOT NULL,
 line double precision NOT NULL, side text NOT NULL CHECK(side IN ('Over','Under')),
 american_price integer NOT NULL, source_updated_at timestamptz NOT NULL,
 player_id text, team_id bigint, match_status text NOT NULL, quote_status text NOT NULL
);
CREATE TABLE IF NOT EXISTS cfb_model_v1.forward_research (
 id bigserial PRIMARY KEY, quote_id bigint NOT NULL UNIQUE REFERENCES cfb_model_v1.prop_quotes(id),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), request jsonb NOT NULL, analysis jsonb NOT NULL,
 market_ready boolean NOT NULL DEFAULT false CHECK (NOT market_ready)
);
