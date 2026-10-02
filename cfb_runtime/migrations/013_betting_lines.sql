-- Consensus closing spread/total per game: the benchmark any game model must be
-- measured against, and a candidate feature. Stored per provider; the model
-- consumes a median consensus so one book's outlier cannot move a fold.
CREATE TABLE IF NOT EXISTS cfb_model_v1.betting_lines (
 game_id bigint NOT NULL REFERENCES cfb_model_v1.games(game_id),
 provider text NOT NULL,
 spread double precision,
 spread_open double precision,
 over_under double precision,
 over_under_open double precision,
 home_moneyline integer,
 away_moneyline integer,
 retrieved_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(game_id,provider)
);
CREATE INDEX IF NOT EXISTS betting_lines_game ON cfb_model_v1.betting_lines(game_id);
