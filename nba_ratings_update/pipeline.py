"""
Pipeline: Read game results from Neon, calculate AdjEM team ratings,
and upsert into nba_team_ratings.
"""

from datetime import datetime

from sqlalchemy import text

from .db import get_engine, query_df
from .ratings import calculate_ratings

SEASON = "2025-26"

UPSERT_SQL = text("""
    INSERT INTO nba_team_ratings
        (team, season, adj_em, adj_off, adj_def, games, last_game_date, calculated_at)
    VALUES
        (:team, :season, :adj_em, :adj_off, :adj_def, :games, :last_game_date, NOW())
    ON CONFLICT (team, season) DO UPDATE SET
        adj_em         = EXCLUDED.adj_em,
        adj_off        = EXCLUDED.adj_off,
        adj_def        = EXCLUDED.adj_def,
        games          = EXCLUDED.games,
        last_game_date = EXCLUDED.last_game_date,
        calculated_at  = NOW()
""")


def run_ratings_update(engine):
    print(f"[{datetime.now().isoformat()}] NBA Ratings Update starting...")

    games_df = query_df("""
        SELECT game_id, game_date, home_team, away_team,
               home_score, away_score, margin, season, status
        FROM nba_game_results
        WHERE season = :season AND status = 'completed'
        ORDER BY game_date
    """, params={"season": SEASON})

    if games_df.empty:
        print("  No completed games found. Exiting.")
        return

    print(f"  Loaded {len(games_df)} completed games for {SEASON}")

    # Use equal weights (no recency) so adj_d/adj_o always get updated regardless of date parsing
    prediction_date = None
    ratings = calculate_ratings(games_df, prediction_date=prediction_date)
    print(f"  Calculated ratings for {len(ratings)} teams")

    # Debug: show adj_off and adj_def for first few teams (should vary, not all 110)
    for r in ratings[:5]:
        print(f"  [sample] {r['team']}: adj_off={r['adj_o']:.2f} adj_def={r['adj_d']:.2f} adj_em={r['adj_em']:.2f}")
    def_range = [r["adj_d"] for r in ratings]
    print(f"  adj_def range: min={min(def_range):.2f} max={max(def_range):.2f} (should not both be 110)")

    last_game = str(games_df["game_date"].max())

    with engine.begin() as conn:
        for r in ratings:
            conn.execute(UPSERT_SQL, {
                "team": r["team"],
                "season": SEASON,
                "adj_em": r["adj_em"],
                "adj_off": r["adj_o"],
                "adj_def": r["adj_d"],
                "games": r["games"],
                "last_game_date": last_game,
            })

    print(f"  Upserted {len(ratings)} ratings to nba_team_ratings")

    # Verify what was written (including adj_def)
    verify = query_df("""
        SELECT team, adj_em, adj_off, adj_def, games FROM nba_team_ratings
        WHERE season = :season ORDER BY adj_em DESC LIMIT 5
    """, params={"season": SEASON})
    print(f"  Top 5 in DB after write:")
    for _, row in verify.iterrows():
        print(f"    {row['team']}: adj_em={row['adj_em']:+.2f} adj_off={row['adj_off']:.2f} adj_def={row['adj_def']:.2f} ({row['games']}g)")
    print("  Done.")
