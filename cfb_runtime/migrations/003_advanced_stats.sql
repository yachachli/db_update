CREATE TABLE IF NOT EXISTS cfb_model_v1.advanced_game_stats (
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 team_id bigint NOT NULL, opponent_id bigint NOT NULL,
 pass_success double precision, rush_success double precision,
 pass_explosiveness double precision, rush_explosiveness double precision,
 clean_plays integer, plays integer,
 clean_payload jsonb NOT NULL, full_payload jsonb NOT NULL,
 retrieved_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(game_id,team_id)
);
