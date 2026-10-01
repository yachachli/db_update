CREATE SCHEMA IF NOT EXISTS cfb_model_v1;
CREATE TABLE IF NOT EXISTS cfb_model_v1.ingestion_runs (
 id bigserial PRIMARY KEY, retrieved_at timestamptz NOT NULL DEFAULT now(),
 provider text NOT NULL, endpoint text NOT NULL, parameters jsonb NOT NULL,
 row_count integer NOT NULL, quota_remaining text
);
CREATE TABLE IF NOT EXISTS cfb_model_v1.games (
 game_id bigint PRIMARY KEY, season integer NOT NULL, week integer NOT NULL,
 season_type text NOT NULL, kickoff timestamptz NOT NULL,
 home_id bigint NOT NULL, away_id bigint NOT NULL, home_team text NOT NULL, away_team text NOT NULL,
 home_classification text, away_classification text, neutral_site boolean,
 completed boolean NOT NULL, home_points integer, away_points integer, payload jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS games_season ON cfb_model_v1.games(season,week);
CREATE TABLE IF NOT EXISTS cfb_model_v1.player_game_stats (
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id), player_id text NOT NULL,
 team text NOT NULL, player_name text NOT NULL, category text NOT NULL, stat_type text NOT NULL,
 stat_value text NOT NULL, PRIMARY KEY(game_id,player_id,category,stat_type)
);
CREATE TABLE IF NOT EXISTS cfb_model_v1.odds_snapshots (
 id bigserial PRIMARY KEY, captured_at timestamptz NOT NULL DEFAULT now(), event_id text NOT NULL, payload jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS cfb_model_v1.prediction_runs (
 id bigserial PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now(), model_version text NOT NULL,
 kind text NOT NULL, configuration jsonb NOT NULL, metrics jsonb
);
CREATE TABLE IF NOT EXISTS cfb_model_v1.game_predictions (
 run_id bigint NOT NULL REFERENCES cfb_model_v1.prediction_runs(id), game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 data_cutoff timestamptz NOT NULL, predicted_home_margin double precision NOT NULL,
 predicted_total double precision NOT NULL, actual_home_margin double precision, actual_total double precision,
 PRIMARY KEY(run_id,game_id)
);
