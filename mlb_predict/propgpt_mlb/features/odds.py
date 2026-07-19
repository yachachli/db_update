"""Load closing market lines from odds_snapshots for backtesting."""
from __future__ import annotations

from typing import Any

import pandas as pd

from propgpt_mlb.db import SCHEMA, fetch_all

PREFERRED_BOOKS = ("draftkings", "fanduel", "betmgm", "caesars")


def american_to_implied_prob(odds: int | float | None) -> float | None:
    """Convert American odds to implied win probability (no vig removal)."""
    if odds is None:
        return None
    ml = float(odds)
    if ml == 0:
        return None
    if ml < 0:
        return (-ml) / ((-ml) + 100.0)
    return 100.0 / (ml + 100.0)


def load_closing_lines(
    *,
    season: int | None = None,
    segment: str = "full_game",
) -> pd.DataFrame:
    """One preferred closing line per game (DraftKings first, else earliest preferred book)."""
    clauses = ["o.segment = %s", "o.total_line IS NOT NULL"]
    params: list[Any] = [segment]
    if season is not None:
        clauses.append("g.season = %s")
        params.append(season)

    where = " AND ".join(clauses)
    book_order = " ".join(
        f"WHEN o.book = '{b}' THEN {i}" for i, b in enumerate(PREFERRED_BOOKS)
    )
    rows = fetch_all(
        f"""
        SELECT DISTINCT ON (o.game_id)
            o.game_id,
            g.game_date,
            o.book,
            o.total_line,
            o.over_odds,
            o.under_odds,
            o.ml_home,
            o.ml_away,
            o.is_closing,
            o.snapshot_time
        FROM {SCHEMA}.odds_snapshots o
        JOIN {SCHEMA}.games g ON g.game_id = o.game_id
        WHERE {where}
        ORDER BY o.game_id,
                 o.is_closing DESC,
                 CASE {book_order} ELSE 99 END,
                 o.snapshot_time DESC
        """,
        tuple(params),
    )
    if not rows:
        return pd.DataFrame(
            columns=[
                "game_id",
                "game_date",
                "book",
                "total_line",
                "over_odds",
                "under_odds",
                "ml_home",
                "ml_away",
                "is_closing",
                "snapshot_time",
            ]
        )
    df = pd.DataFrame(rows)
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["total_line"] = df["total_line"].astype(float)
    df["mkt_home_prob"] = df["ml_home"].map(american_to_implied_prob)
    df["mkt_away_prob"] = df["ml_away"].map(american_to_implied_prob)
    return df


def odds_coverage_summary(season: int) -> dict[str, int]:
    """Games with outcomes vs games with a closing total line."""
    row = fetch_all(
        f"""
        SELECT
            COUNT(DISTINCT g.game_id) AS games_with_outcomes,
            COUNT(DISTINCT o.game_id) AS games_with_odds
        FROM {SCHEMA}.games g
        JOIN {SCHEMA}.outcomes oc ON g.game_id = oc.game_id
        LEFT JOIN {SCHEMA}.odds_snapshots o
            ON g.game_id = o.game_id
           AND o.segment = 'full_game'
           AND o.total_line IS NOT NULL
        WHERE g.season = %s
        """,
        (season,),
    )[0]
    return {
        "games_with_outcomes": int(row["games_with_outcomes"]),
        "games_with_odds": int(row["games_with_odds"]),
    }
