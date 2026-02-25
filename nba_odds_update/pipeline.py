"""
Pipeline: Fetch live/upcoming NBA odds from The Odds API and upsert
into nba_historical_odds in Neon.

Should run BEFORE games start each day to capture closing lines.
"""

import os
from datetime import datetime, timedelta

from sqlalchemy import text

from .db import get_engine, query_df
from .odds_client import OddsClient, TEAM_NAME_TO_ABV
from .game_parser import derive_season

UPSERT_SQL = text("""
    INSERT INTO nba_historical_odds
        (game_date, home_team, away_team, spread,
         home_moneyline, total, source, season, captured_at)
    VALUES
        (:game_date, :home_team, :away_team, :spread,
         :home_moneyline, :total, :source, :season, NOW())
    ON CONFLICT (game_date, home_team, away_team, source) DO UPDATE SET
        spread         = EXCLUDED.spread,
        home_moneyline = EXCLUDED.home_moneyline,
        total          = EXCLUDED.total,
        captured_at    = NOW()
""")


def run_odds_update(engine):
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        raise RuntimeError("ODDS_API_KEY not set")

    print(f"[{datetime.now().isoformat()}] NBA Odds Update starting...")

    client = OddsClient(api_key)
    events = client.get_live_odds()

    if not events:
        print("  No odds events returned from API.")
        return

    print(f"  Received {len(events)} events from API")
    total = 0

    with engine.begin() as conn:
        for event in events:
            home_full = event.get("home_team", "")
            away_full = event.get("away_team", "")
            home = TEAM_NAME_TO_ABV.get(home_full)
            away = TEAM_NAME_TO_ABV.get(away_full)
            if not home or not away:
                continue

            commence = event.get("commence_time", "")
            try:
                dt = datetime.fromisoformat(commence.replace("Z", "+00:00"))
                game_date = (dt - timedelta(hours=5)).strftime("%Y-%m-%d")
            except Exception:
                game_date = datetime.now().strftime("%Y-%m-%d")

            spreads, mls, totals = [], [], []
            for bm in event.get("bookmakers", []):
                for mkt in bm.get("markets", []):
                    if mkt["key"] == "spreads":
                        for oc in mkt.get("outcomes", []):
                            if oc["name"] == home_full and "point" in oc:
                                spreads.append(oc["point"])
                    elif mkt["key"] == "h2h":
                        for oc in mkt.get("outcomes", []):
                            if oc["name"] == home_full:
                                mls.append(oc["price"])
                    elif mkt["key"] == "totals":
                        for oc in mkt.get("outcomes", []):
                            if oc["name"] == "Over" and "point" in oc:
                                totals.append(oc["point"])

            if not spreads:
                continue

            conn.execute(UPSERT_SQL, {
                "game_date": game_date,
                "home_team": home,
                "away_team": away,
                "spread": round(sum(spreads) / len(spreads), 1),
                "home_moneyline": round(sum(mls) / len(mls)) if mls else None,
                "total": round(sum(totals) / len(totals), 1) if totals else None,
                "source": "consensus",
                "season": derive_season(game_date.replace("-", "")),
            })
            total += 1

    summary = query_df("""
        SELECT COUNT(*) AS total_rows, MAX(game_date) AS latest
        FROM nba_historical_odds
    """)
    print(f"  {total} odds lines upserted")
    print(f"  Neon total: {summary['total_rows'].iloc[0]} rows, "
          f"latest: {summary['latest'].iloc[0]}")
    print("  Done.")
