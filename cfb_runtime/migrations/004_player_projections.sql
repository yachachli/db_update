CREATE TABLE IF NOT EXISTS cfb_model_v1.player_offense (
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 player_id text NOT NULL, team_id bigint NOT NULL, player_name text NOT NULL,
 category text NOT NULL, volume integer NOT NULL CHECK(volume>=0), yards integer NOT NULL,
 PRIMARY KEY(game_id,player_id,category)
);
CREATE TABLE IF NOT EXISTS cfb_model_v1.player_predictions (
 run_id bigint NOT NULL REFERENCES cfb_model_v1.prediction_runs(id),
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 player_id text NOT NULL, team_id bigint NOT NULL, category text NOT NULL,
 data_cutoff timestamptz NOT NULL, predicted_volume double precision NOT NULL,
 predicted_yards double precision NOT NULL, baseline_yards double precision NOT NULL,
 actual_volume integer, actual_yards integer, features jsonb NOT NULL,
 PRIMARY KEY(run_id,game_id,player_id,category)
);
