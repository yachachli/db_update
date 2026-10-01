CREATE TABLE IF NOT EXISTS cfb_model_v1.play_label_audits (
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 team_id bigint NOT NULL, passed boolean NOT NULL, evidence jsonb NOT NULL,
 retrieved_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(game_id,team_id)
);
CREATE TABLE IF NOT EXISTS cfb_model_v1.play_derived_zeros (
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 team_id bigint NOT NULL, player_id text NOT NULL, targets integer NOT NULL CHECK(targets>0),
 category text NOT NULL CHECK(category='receiving'),
 provenance text NOT NULL DEFAULT 'reconciled_play_targets_no_receptions',
 PRIMARY KEY(game_id,team_id,player_id)
);
