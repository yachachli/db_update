-- Player and team directory for the app's search and card surfaces.
--
-- Image URLs are stored, not mirrored binaries: every other league in this
-- database keeps a URL on its players table and lets the client fetch it.
-- ESPN serves college headshots and NCAA logos at deterministic paths keyed by
-- the CFBD id, which is the ESPN id, so there is nothing to scrape. What is
-- worth recording is which of those paths actually resolve, so the app is never
-- handed a URL that 404s.
CREATE TABLE IF NOT EXISTS cfb_model_v1.team_directory (
 team_id bigint PRIMARY KEY, school text NOT NULL, abbreviation text,
 conference text, color text, alternate_color text,
 logo_url text, updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cfb_model_v1.player_directory (
 player_id text PRIMARY KEY, player_name text NOT NULL,
 first_name text, last_name text, position text,
 team_id bigint, team text, jersey integer,
 -- NULL headshot_url with a non-null checked_at means ESPN has no portrait for
 -- this player, which is different from never having looked.
 headshot_url text, headshot_checked_at timestamptz,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS player_directory_team ON cfb_model_v1.player_directory(team_id);
CREATE INDEX IF NOT EXISTS player_directory_unchecked
  ON cfb_model_v1.player_directory(headshot_checked_at NULLS FIRST);
