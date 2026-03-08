-- BracketIQ Neon PostgreSQL schema. Run once in Neon console to create all tables.
-- push_to_neon.py uses if_exists='replace' for full refresh each night.

-- KenPom ratings (core — used by slate and predictions)
CREATE TABLE IF NOT EXISTS kenpom_ratings (
    team TEXT PRIMARY KEY,
    conference TEXT,
    rank INTEGER,
    adj_oe FLOAT,
    adj_de FLOAT,
    adj_em FLOAT,
    adj_tempo FLOAT,
    luck FLOAT,
    sos_adj_em FLOAT,
    sos_adj_oe FLOAT,
    sos_adj_de FLOAT,
    ncsos_adj_em FLOAT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Four factors (team profiles, analysis)
CREATE TABLE IF NOT EXISTS kenpom_fourfactors (
    team TEXT PRIMARY KEY,
    conference TEXT,
    adj_tempo FLOAT,
    adj_oe FLOAT,
    off_efg FLOAT,
    off_to FLOAT,
    off_or FLOAT,
    off_ft_rate FLOAT,
    adj_de FLOAT,
    def_efg FLOAT,
    def_to FLOAT,
    def_or FLOAT,
    def_ft_rate FLOAT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Team stats offense
CREATE TABLE IF NOT EXISTS kenpom_teamstats_off (
    team TEXT PRIMARY KEY,
    conference TEXT,
    three_p_pct FLOAT,
    two_p_pct FLOAT,
    ft_pct FLOAT,
    blk_pct FLOAT,
    stl_pct FLOAT,
    a_pct FLOAT,
    three_pa_pct FLOAT,
    adj_oe FLOAT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Team stats defense
CREATE TABLE IF NOT EXISTS kenpom_teamstats_def (
    team TEXT PRIMARY KEY,
    conference TEXT,
    three_p_pct FLOAT,
    two_p_pct FLOAT,
    ft_pct FLOAT,
    blk_pct FLOAT,
    stl_pct FLOAT,
    a_pct FLOAT,
    three_pa_pct FLOAT,
    adj_de FLOAT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Height / experience
CREATE TABLE IF NOT EXISTS kenpom_height (
    team TEXT PRIMARY KEY,
    conference TEXT,
    avg_hgt FLOAT,
    eff_hgt FLOAT,
    experience FLOAT,
    bench_minutes FLOAT,
    continuity FLOAT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Today's slate (refreshed each run)
CREATE TABLE IF NOT EXISTS slate_today (
    id SERIAL PRIMARY KEY,
    game_date TEXT,
    home_team TEXT,
    away_team TEXT,
    home_team_kenpom TEXT,
    away_team_kenpom TEXT,
    kenpom_predicted_margin_home_pov FLOAT,
    kenpom_win_prob_home FLOAT,
    kenpom_predicted_total FLOAT,
    vegas_spread_home_pov FLOAT,
    vegas_total FLOAT,
    vegas_implied_prob_home FLOAT,
    vegas_implied_prob_away FLOAT,
    spread_edge FLOAT,
    spread_edge_confidence TEXT,
    historical_cover_rate TEXT,
    moneyline_edge FLOAT,
    over_under_edge FLOAT,
    slow_underdog_flag BOOLEAN DEFAULT FALSE,
    slow_underdog_note TEXT,
    spread_edge_interpretation TEXT,
    moneyline_edge_interpretation TEXT,
    ou_edge_interpretation TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ATS historical (rebuilt nightly)
CREATE TABLE IF NOT EXISTS ats_historical (
    id SERIAL PRIMARY KEY,
    game_date TEXT,
    home_team TEXT,
    away_team TEXT,
    home_rank INTEGER,
    away_rank INTEGER,
    kenpom_predicted_margin FLOAT,
    vegas_spread FLOAT,
    actual_margin_home FLOAT,
    covered_vegas BOOLEAN,
    covered_kenpom BOOLEAN,
    kenpom_vs_vegas_edge FLOAT,
    vegas_total FLOAT,
    actual_total FLOAT,
    over_under_result TEXT
);

-- FanMatch historical
CREATE TABLE IF NOT EXISTS fanmatch_historical (
    id SERIAL PRIMARY KEY,
    fanmatch_date TEXT,
    game TEXT,
    predicted_winner TEXT,
    predicted_loser TEXT,
    predicted_mov FLOAT,
    winner TEXT,
    loser TEXT,
    winner_score TEXT,
    loser_score TEXT,
    actual_mov FLOAT,
    location TEXT,
    thrill_score TEXT
);

-- Odds historical
CREATE TABLE IF NOT EXISTS odds_historical (
    id SERIAL PRIMARY KEY,
    game_date TEXT,
    home_team TEXT,
    away_team TEXT,
    home_team_kenpom TEXT,
    away_team_kenpom TEXT,
    consensus_spread FLOAT,
    consensus_total FLOAT,
    num_bookmakers INTEGER
);
