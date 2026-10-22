CREATE SCHEMA IF NOT EXISTS propgpt_evidence;
CREATE TABLE IF NOT EXISTS propgpt_evidence.wnba_events (
    observation_id text PRIMARY KEY,
    run_id text NOT NULL,
    endpoint text NOT NULL,
    game_id text NOT NULL,
    game_date date NOT NULL,
    event_time timestamptz,
    is_final boolean NOT NULL,
    home_score integer,
    away_score integer,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS wnba_event_observed ON propgpt_evidence.wnba_events(observed_at);
CREATE INDEX IF NOT EXISTS wnba_event_game ON propgpt_evidence.wnba_events(game_id,observed_at);
