CREATE TABLE IF NOT EXISTS cfb_model_v1.player_outcome_audits (
 audit_run_id bigint NOT NULL REFERENCES cfb_model_v1.prediction_runs(id),
 source_run_id bigint NOT NULL REFERENCES cfb_model_v1.prediction_runs(id),
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 player_id text NOT NULL, team_id bigint NOT NULL, category text NOT NULL,
 status text NOT NULL, verified_yards integer, sensitivity_yards integer,
 evidence jsonb NOT NULL,
 PRIMARY KEY(audit_run_id,source_run_id,game_id,player_id,category)
);
