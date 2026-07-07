"""Pipeline for mlb_odds_update.

Snapshots sportsbook odds for today's slate into propgpt_mlb.odds_snapshots,
covering both full-game markets (moneyline / run line / totals) and
first-5-innings (F5) markets. Runs several times a day; each run is one snapshot
instant, so the rows form a per-game/book time series (the latest snapshot before
first pitch becomes the closing line for CLV, handled later).

SCHEMA: every table reference is qualified with `propgpt_mlb.` (via the SCHEMA
constant) — Neon's pooled endpoint runs PgBouncer in transaction mode and does not
preserve search_path across transactions.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import Engine, create_engine, text

from .odds_api_client import OddsAPIClient

SCHEMA = "propgpt_mlb"

logger = logging.getLogger("mlb_odds_update.pipeline")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

# MLB slates are ET-centric — a night game at 10pm PT is still "today" locally, so we
# bucket events by their US/Eastern calendar date rather than UTC.
ET_ZONE = ZoneInfo("America/New_York")

# Books we keep. Everything else is dropped to control noise + credit cost.
# NOTE: The Odds API still returns Caesars under the legacy `williamhill_us` key, so we
# normalise it to `caesars` before the allowlist check and storage.
BOOKMAKER_ALLOWLIST = {"draftkings", "fanduel", "betmgm", "caesars"}
BOOK_KEY_ALIASES = {"williamhill_us": "caesars"}

# Per-segment market keys. Full-game markets come from the bulk endpoint; the F5
# variants are game-period markets fetched per event.
MARKET_KEYS: dict[str, dict[str, str]] = {
    "full_game": {"h2h": "h2h", "spreads": "spreads", "totals": "totals"},
    "f5": {
        "h2h": "h2h_1st_5_innings",
        "spreads": "spreads_1st_5_innings",
        "totals": "totals_1st_5_innings",
    },
}


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

def get_engine() -> Engine:
    """SQLAlchemy Engine pointed at Neon. No search_path set — all SQL is schema-qualified."""
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set — check GitHub Secrets.")

    if url.startswith("postgresql://") and "+" not in url.split("://", 1)[0]:
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)

    return create_engine(url, pool_pre_ping=True)


# ---------------------------------------------------------------------------
# Date logic (explicit + doctested — MLB slates bucket by ET calendar date)
# ---------------------------------------------------------------------------

def event_et_date(commence_time_iso: str | None) -> date | None:
    """Return the US/Eastern calendar date of a UTC commence_time from The Odds API.

    The Odds API returns UTC ISO-8601 timestamps like "2026-07-07T18:16:00Z".

    >>> event_et_date("2026-07-07T18:16:00Z").isoformat()  # 2:16pm ET, same day
    '2026-07-07'
    >>> event_et_date("2026-07-08T02:16:00Z").isoformat()  # 10:16pm ET on the 7th
    '2026-07-07'
    >>> event_et_date("2026-07-07T03:30:00Z").isoformat()  # 11:30pm ET on the 6th
    '2026-07-06'
    >>> event_et_date(None) is None
    True
    """
    if not commence_time_iso:
        return None
    dt = datetime.fromisoformat(commence_time_iso.replace("Z", "+00:00"))
    return dt.astimezone(ET_ZONE).date()


def default_target_date() -> date:
    """Today's date in US/Eastern."""
    return datetime.now(ET_ZONE).date()


def _parse_utc(iso: str | None) -> datetime | None:
    if not iso:
        return None
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Lookups against propgpt_mlb
# ---------------------------------------------------------------------------

def build_team_lookup(engine: Engine) -> dict[str, int]:
    """Map full team name ("Seattle Mariners") -> team_id. Built once per run."""
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT team_id, name FROM {SCHEMA}.teams")).fetchall()
    return {r.name: r.team_id for r in rows}


def get_games_for_date(engine: Engine, target_date: date) -> list[dict[str, Any]]:
    """All games scheduled on target_date, with team ids + scheduled start (for DH match)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT game_id, home_team_id, away_team_id, game_time_utc
                FROM {SCHEMA}.games
                WHERE game_date = :d
            """),
            {"d": target_date},
        ).fetchall()
    return [
        {
            "game_id": r.game_id,
            "home_team_id": r.home_team_id,
            "away_team_id": r.away_team_id,
            "game_time_utc": r.game_time_utc,
        }
        for r in rows
    ]


def match_event_to_game(
    event: dict[str, Any],
    home_id: int,
    away_id: int,
    games_by_teams: dict[tuple[int, int], list[dict[str, Any]]],
) -> int | None:
    """Resolve an odds event to a game_id via (home_id, away_id).

    Doubleheaders (multiple games with the same teams on the same date) are
    disambiguated by closest scheduled start to the event's commence_time; if that
    can't be decided, we return None (skip) rather than guess.
    """
    candidates = games_by_teams.get((home_id, away_id), [])
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]["game_id"]

    # Doubleheader: pick the game whose scheduled start is closest to commence_time.
    commence = _parse_utc(event.get("commence_time"))
    if commence is None:
        logger.warning("Doubleheader for teams (%s,%s) but event has no commence_time — skipping",
                       away_id, home_id)
        return None

    scored: list[tuple[float, int]] = []
    for g in candidates:
        start = g["game_time_utc"]
        if start is None:
            continue
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        scored.append((abs((start - commence).total_seconds()), g["game_id"]))

    if not scored:
        logger.warning("Doubleheader for teams (%s,%s) but no game has a start time — skipping",
                       away_id, home_id)
        return None

    scored.sort(key=lambda x: x[0])
    if len(scored) >= 2 and scored[0][0] == scored[1][0]:
        logger.warning("Doubleheader for teams (%s,%s) is ambiguous (equal time distance) — skipping",
                       away_id, home_id)
        return None
    return scored[0][1]


# ---------------------------------------------------------------------------
# Market parsing — map The Odds API markets to odds_snapshots columns
# ---------------------------------------------------------------------------

def extract_book_odds(
    markets: list[dict[str, Any]],
    segment: str,
    home_name: str,
    away_name: str,
) -> tuple[dict[str, Any], bool]:
    """Flatten one bookmaker's markets into odds_snapshots columns for a segment.

    Returns (values, found) where found is False if none of the segment's three
    markets were present (caller then skips this book).
    Mapping: h2h -> ml_home/ml_away, spreads -> run line, totals -> total + over/under.
    """
    keys = MARKET_KEYS[segment]
    by_key = {m.get("key"): m for m in markets}
    vals: dict[str, Any] = {
        "total_line": None, "over_odds": None, "under_odds": None,
        "ml_home": None, "ml_away": None,
        "rl_home_spread": None, "rl_home_odds": None, "rl_away_odds": None,
    }
    found = False

    h2h = by_key.get(keys["h2h"])
    if h2h:
        found = True
        for o in h2h.get("outcomes", []):
            if o.get("name") == home_name:
                vals["ml_home"] = o.get("price")
            elif o.get("name") == away_name:
                vals["ml_away"] = o.get("price")

    spreads = by_key.get(keys["spreads"])
    if spreads:
        found = True
        for o in spreads.get("outcomes", []):
            if o.get("name") == home_name:
                vals["rl_home_spread"] = o.get("point")
                vals["rl_home_odds"] = o.get("price")
            elif o.get("name") == away_name:
                vals["rl_away_odds"] = o.get("price")

    totals = by_key.get(keys["totals"])
    if totals:
        found = True
        for o in totals.get("outcomes", []):
            if o.get("name") == "Over":
                vals["over_odds"] = o.get("price")
                vals["total_line"] = o.get("point")
            elif o.get("name") == "Under":
                vals["under_odds"] = o.get("price")
                if vals["total_line"] is None:
                    vals["total_line"] = o.get("point")

    return vals, found


# ---------------------------------------------------------------------------
# Writer — schema-qualified, idempotent on the segment-aware unique index
# ---------------------------------------------------------------------------

def upsert_odds_snapshot(
    engine: Engine,
    *,
    game_id: int,
    book: str,
    segment: str,
    snapshot_time: datetime,
    odds_event_id: str | None,
    odds: dict[str, Any],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.odds_snapshots (
                    game_id, book, segment, snapshot_time, odds_event_id,
                    total_line, over_odds, under_odds,
                    ml_home, ml_away,
                    rl_home_spread, rl_home_odds, rl_away_odds
                )
                VALUES (
                    :game_id, :book, :segment, :snapshot_time, :odds_event_id,
                    :total_line, :over_odds, :under_odds,
                    :ml_home, :ml_away,
                    :rl_home_spread, :rl_home_odds, :rl_away_odds
                )
                ON CONFLICT (game_id, book, segment, snapshot_time) DO UPDATE SET
                    odds_event_id = EXCLUDED.odds_event_id,
                    total_line = EXCLUDED.total_line,
                    over_odds = EXCLUDED.over_odds,
                    under_odds = EXCLUDED.under_odds,
                    ml_home = EXCLUDED.ml_home,
                    ml_away = EXCLUDED.ml_away,
                    rl_home_spread = EXCLUDED.rl_home_spread,
                    rl_home_odds = EXCLUDED.rl_home_odds,
                    rl_away_odds = EXCLUDED.rl_away_odds
            """),
            {
                "game_id": game_id,
                "book": book,
                "segment": segment,
                "snapshot_time": snapshot_time,
                "odds_event_id": odds_event_id,
                **odds,
            },
        )


def count_rows(engine: Engine, table: str) -> int:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) AS n FROM {SCHEMA}.{table}")).fetchone()
        return int(row.n) if row else 0


def log_row_counts(engine: Engine) -> None:
    logger.info("Row counts: %s", {"odds_snapshots": count_rows(engine, "odds_snapshots")})


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _upsert_book_segment(
    engine: Engine,
    *,
    game_id: int,
    event_id: str | None,
    bookmakers: list[dict[str, Any]],
    segment: str,
    snapshot_time: datetime,
    home_name: str,
    away_name: str,
) -> int:
    """Upsert one segment's rows for all allowlisted books of an event. Returns row count."""
    written = 0
    for bk in bookmakers:
        book = BOOK_KEY_ALIASES.get(bk.get("key"), bk.get("key"))
        if book not in BOOKMAKER_ALLOWLIST:
            continue
        odds, found = extract_book_odds(bk.get("markets", []), segment, home_name, away_name)
        if not found:
            continue
        upsert_odds_snapshot(
            engine,
            game_id=game_id,
            book=book,
            segment=segment,
            snapshot_time=snapshot_time,
            odds_event_id=event_id,
            odds=odds,
        )
        written += 1
    return written


def sync_odds_for_date(engine: Engine, target_date: date) -> dict[str, Any]:
    """Snapshot full-game + F5 odds for target_date's (ET) slate into odds_snapshots."""
    client = OddsAPIClient(os.getenv("ODDS_API_KEY", ""))
    snapshot_time = datetime.now(timezone.utc)

    team_lookup = build_team_lookup(engine)
    games = get_games_for_date(engine, target_date)
    games_by_teams: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for g in games:
        games_by_teams.setdefault((g["home_team_id"], g["away_team_id"]), []).append(g)
    logger.info("Loaded %d games and %d teams for %s", len(games), len(team_lookup), target_date)

    events = client.get_featured_odds()
    logger.info("Fetched %d odds events (whole board)", len(events))

    events_on_date = 0
    events_matched = 0
    events_unmatched = 0
    unresolved_names: set[str] = set()
    full_game_rows = 0
    f5_rows = 0
    games_without_f5 = 0

    for event in events:
        if event_et_date(event.get("commence_time")) != target_date:
            continue
        events_on_date += 1

        home_name = event.get("home_team")
        away_name = event.get("away_team")
        home_id = team_lookup.get(home_name)
        away_id = team_lookup.get(away_name)
        if home_id is None or away_id is None:
            for nm, rid in ((home_name, home_id), (away_name, away_id)):
                if rid is None:
                    unresolved_names.add(nm)
            logger.error("UNRESOLVED team name(s) for event %s: home=%r away=%r — skipping",
                         event.get("id"), home_name, away_name)
            events_unmatched += 1
            continue

        game_id = match_event_to_game(event, home_id, away_id, games_by_teams)
        if game_id is None:
            logger.warning("No game match for %s @ %s on %s (event %s) — skipping",
                           away_name, home_name, target_date, event.get("id"))
            events_unmatched += 1
            continue

        events_matched += 1
        event_id = event.get("id")

        # Full-game snapshots (from the bulk response we already have).
        full_game_rows += _upsert_book_segment(
            engine,
            game_id=game_id,
            event_id=event_id,
            bookmakers=event.get("bookmakers", []),
            segment="full_game",
            snapshot_time=snapshot_time,
            home_name=home_name,
            away_name=away_name,
        )

        # F5 snapshots (one extra call per matched event).
        try:
            f5_event = client.get_event_f5_odds(event_id)
        except Exception as e:
            logger.warning("F5 fetch failed for event %s (%s @ %s): %s",
                           event_id, away_name, home_name, e)
            games_without_f5 += 1
            continue

        rows = _upsert_book_segment(
            engine,
            game_id=game_id,
            event_id=event_id,
            bookmakers=f5_event.get("bookmakers", []),
            segment="f5",
            snapshot_time=snapshot_time,
            home_name=home_name,
            away_name=away_name,
        )
        f5_rows += rows
        if rows == 0:
            games_without_f5 += 1
            logger.info("No F5 lines yet for %s @ %s (event %s)", away_name, home_name, event_id)

    if unresolved_names:
        logger.error("Team names that did not resolve to propgpt_mlb.teams: %s",
                     sorted(unresolved_names))

    logger.info(
        "Odds for %s — events on date: %d, matched: %d, unmatched: %d, "
        "full_game rows: %d, F5 rows: %d, games without F5: %d | credits used: %s, remaining: %s",
        target_date, events_on_date, events_matched, events_unmatched,
        full_game_rows, f5_rows, games_without_f5,
        client.requests_used, client.requests_remaining,
    )

    return {
        "events_on_date": events_on_date,
        "events_matched": events_matched,
        "events_unmatched": events_unmatched,
        "unresolved_team_names": sorted(unresolved_names),
        "full_game_rows": full_game_rows,
        "f5_rows": f5_rows,
        "games_without_f5": games_without_f5,
        "credits_used": client.requests_used,
        "credits_remaining": client.requests_remaining,
    }
