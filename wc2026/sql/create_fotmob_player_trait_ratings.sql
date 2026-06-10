-- WC 2026 FotMob player trait ratings (one row per team).

CREATE TABLE IF NOT EXISTS fotmob_player_trait_ratings (
    team              TEXT PRIMARY KEY,
    "group"           TEXT NOT NULL,
    player_name       TEXT,
    player_rank_used  INTEGER NOT NULL,
    fotmob_id         BIGINT,
    fotmob_url        TEXT,
    compared_to       TEXT,
    has_traits        BOOLEAN NOT NULL DEFAULT FALSE,
    trait1_name       TEXT,
    trait1_pct        INTEGER,
    trait2_name       TEXT,
    trait2_pct        INTEGER,
    trait3_name       TEXT,
    trait3_pct        INTEGER,
    trait4_name       TEXT,
    trait4_pct        INTEGER,
    trait5_name       TEXT,
    trait5_pct        INTEGER,
    trait6_name       TEXT,
    trait6_pct        INTEGER,
    traits_json       TEXT,
    scraped_at        TIMESTAMPTZ,
    data_source       TEXT NOT NULL DEFAULT 'fotmob_playerData'
);

-- From psql (adjust path to your checkout):
-- \copy fotmob_player_trait_ratings FROM 'data/fotmob_player_trait_ratings.csv' CSV HEADER;
