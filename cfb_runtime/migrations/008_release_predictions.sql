CREATE TABLE IF NOT EXISTS cfb_model_v1.release_predictions (
 run_id bigint NOT NULL REFERENCES cfb_model_v1.prediction_runs(id),
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 player_id text NOT NULL, category text NOT NULL,
 predicted_yards double precision NOT NULL, lower90 double precision NOT NULL, upper90 double precision NOT NULL,
 reference_line double precision NOT NULL, conditional_over_probability double precision NOT NULL,
 actual_yards integer, market_abstain boolean NOT NULL CHECK(market_abstain),
 PRIMARY KEY(run_id,game_id,player_id,category)
);
