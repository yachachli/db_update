CREATE TABLE IF NOT EXISTS cfb_model_v1.team_game_stats (
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 team_id bigint NOT NULL, opponent_id bigint NOT NULL,
 pass_yards integer, pass_attempts integer CHECK (pass_attempts >= 0),
 rush_yards integer, rush_attempts integer CHECK (rush_attempts >= 0),
 payload jsonb NOT NULL, retrieved_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(game_id,team_id)
);
CREATE TABLE IF NOT EXISTS cfb_model_v1.game_features (
 run_id bigint NOT NULL REFERENCES cfb_model_v1.prediction_runs(id),
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 data_cutoff timestamptz NOT NULL, features jsonb NOT NULL,
 PRIMARY KEY(run_id,game_id)
);
