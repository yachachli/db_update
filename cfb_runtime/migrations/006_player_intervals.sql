CREATE TABLE IF NOT EXISTS cfb_model_v1.player_intervals (
 run_id bigint NOT NULL REFERENCES cfb_model_v1.prediction_runs(id),
 source_run_id bigint NOT NULL REFERENCES cfb_model_v1.prediction_runs(id),
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 player_id text NOT NULL, category text NOT NULL,
 predicted_yards double precision NOT NULL,
 lower_yards double precision NOT NULL, upper_yards double precision NOT NULL,
 actual_yards integer, research_eligible boolean NOT NULL,
 market_abstain boolean NOT NULL DEFAULT true CHECK(market_abstain),
 abstention_reasons jsonb NOT NULL,
 PRIMARY KEY(run_id,game_id,player_id,category),
 CHECK(lower_yards<=upper_yards)
);
