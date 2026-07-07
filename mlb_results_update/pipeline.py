"""Pipeline for mlb_results_update.

Owns the SQLAlchemy engine, writer SQL, and the orchestration logic for
ingesting completed-game data into the propgpt_mlb schema.

SCHEMA: every table reference is qualified with `propgpt_mlb.` (via the
SCHEMA constant) for compatibility with Neon's pooled endpoint.

SCHEMA SYNC: writer SQL here parallels other folders that write to the same
tables. When the propgpt_mlb schema changes, update sibling folders too.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

from sqlalchemy import Engine, create_engine, text

from .mlb_stats_client import MLBStatsClient

SCHEMA = "propgpt_mlb"

logger = logging.getLogger("mlb_results_update.pipeline")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)


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
# Helpers
# ---------------------------------------------------------------------------

def _ip_to_decimal(ip_str: str | float | None) -> float | None:
    """Convert MLB's IP format ('5.2' = 5 and 2/3 innings) to decimal NUMERIC(4,1).

    MLB uses a weird format: the digit after the decimal is in *thirds*, not tenths.
    - '5.0' = 5.0 IP
    - '5.1' = 5.333... IP (5 and 1/3)
    - '5.2' = 5.667... IP (5 and 2/3)
    - '6.0' = 6.0 IP

    Our schema stores NUMERIC(4,1) so we preserve MLB's display format directly
    rather than converting to true decimal. Downstream feature code can convert
    if it needs true innings.
    """
    if ip_str is None or ip_str == "":
        return None
    try:
        return float(ip_str)
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def compute_f5_runs(innings: list[dict[str, Any]]) -> tuple[int | None, int | None]:
    """Sum home/away runs across innings 1–5 from an MLB Stats linescore innings array.

    Returns (home_runs_f5, away_runs_f5), or (None, None) if fewer than 5 innings are
    recorded (rain-shortened / suspended game) — the caller logs and stores NULLs rather
    than guessing. A missing half-inning entry (e.g. home didn't bat) counts as 0 runs.
    """
    by_num: dict[int, dict[str, Any]] = {}
    for inning in innings:
        num = _safe_int(inning.get("num"))
        if num is not None:
            by_num[num] = inning

    if not all(n in by_num for n in range(1, 6)):
        return None, None

    home_f5 = sum((_safe_int((by_num[n].get("home") or {}).get("runs")) or 0) for n in range(1, 6))
    away_f5 = sum((_safe_int((by_num[n].get("away") or {}).get("runs")) or 0) for n in range(1, 6))
    return home_f5, away_f5


def count_rows(engine: Engine, table: str) -> int:
    """Hardcoded-table-name count helper. Only call with literals."""
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) AS n FROM {SCHEMA}.{table}")).fetchone()
        return int(row.n) if row else 0


def log_row_counts(engine: Engine) -> None:
    counts = {
        "outcomes": count_rows(engine, "outcomes"),
        "pitcher_game_logs": count_rows(engine, "pitcher_game_logs"),
        "team_game_logs": count_rows(engine, "team_game_logs"),
        "players": count_rows(engine, "players"),
    }
    logger.info("Row counts: %s", counts)


# ---------------------------------------------------------------------------
# Writers — schema-qualified, idempotent via ON CONFLICT DO UPDATE
# ---------------------------------------------------------------------------

def upsert_outcome(
    engine: Engine,
    *,
    game_id: int,
    home_score: int,
    away_score: int,
    extra_innings: bool,
    final_innings: float,
    home_runs_f5: int | None = None,
    away_runs_f5: int | None = None,
    linescore: list[dict[str, Any]] | None = None,
) -> None:
    """total_runs, home_won and total_runs_f5 are GENERATED columns — do not write them."""
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.outcomes (
                    game_id, home_score, away_score, extra_innings, final_innings,
                    home_runs_f5, away_runs_f5, linescore
                )
                VALUES (
                    :game_id, :home_score, :away_score, :extra_innings, :final_innings,
                    :home_runs_f5, :away_runs_f5, CAST(:linescore AS JSONB)
                )
                ON CONFLICT (game_id) DO UPDATE SET
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    extra_innings = EXCLUDED.extra_innings,
                    final_innings = EXCLUDED.final_innings,
                    home_runs_f5 = EXCLUDED.home_runs_f5,
                    away_runs_f5 = EXCLUDED.away_runs_f5,
                    linescore = EXCLUDED.linescore,
                    recorded_at = NOW()
            """),
            {
                "game_id": game_id,
                "home_score": home_score,
                "away_score": away_score,
                "extra_innings": extra_innings,
                "final_innings": final_innings,
                "home_runs_f5": home_runs_f5,
                "away_runs_f5": away_runs_f5,
                "linescore": json.dumps(linescore) if linescore is not None else None,
            },
        )


def bump_game_status_to_final(engine: Engine, game_id: int) -> int:
    """Mark a completed game's status as Final. Returns the number of rows changed (0 or 1).

    The `IS DISTINCT FROM 'Final'` guard keeps this idempotent: re-runs against a game
    that is already Final touch no rows and are counted as 0 bumps.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"""
                UPDATE {SCHEMA}.games
                SET status = 'Final', updated_at = NOW()
                WHERE game_id = :game_id AND status IS DISTINCT FROM 'Final'
            """),
            {"game_id": game_id},
        )
        return result.rowcount or 0


def upsert_pitcher_game_log(
    engine: Engine,
    *,
    player_id: int,
    game_id: int,
    team_id: int,
    is_starter: bool,
    pitching_stats: dict[str, Any],
) -> None:
    """Upsert one pitcher's game line from MLB Stats boxscore pitching stats."""
    ps = pitching_stats
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.pitcher_game_logs (
                    player_id, game_id, team_id, is_starter,
                    ip, batters_faced, pitches, strikes,
                    h, r, er, bb, k, hr, hbp,
                    ground_balls, fly_balls, line_drives
                )
                VALUES (
                    :player_id, :game_id, :team_id, :is_starter,
                    :ip, :batters_faced, :pitches, :strikes,
                    :h, :r, :er, :bb, :k, :hr, :hbp,
                    :ground_balls, :fly_balls, :line_drives
                )
                ON CONFLICT (player_id, game_id) DO UPDATE SET
                    team_id = EXCLUDED.team_id,
                    is_starter = EXCLUDED.is_starter,
                    ip = EXCLUDED.ip,
                    batters_faced = EXCLUDED.batters_faced,
                    pitches = EXCLUDED.pitches,
                    strikes = EXCLUDED.strikes,
                    h = EXCLUDED.h,
                    r = EXCLUDED.r,
                    er = EXCLUDED.er,
                    bb = EXCLUDED.bb,
                    k = EXCLUDED.k,
                    hr = EXCLUDED.hr,
                    hbp = EXCLUDED.hbp,
                    ground_balls = EXCLUDED.ground_balls,
                    fly_balls = EXCLUDED.fly_balls,
                    line_drives = EXCLUDED.line_drives
            """),
            {
                "player_id": player_id,
                "game_id": game_id,
                "team_id": team_id,
                "is_starter": is_starter,
                "ip": _ip_to_decimal(ps.get("inningsPitched")),
                "batters_faced": _safe_int(ps.get("battersFaced")),
                "pitches": _safe_int(ps.get("numberOfPitches")),
                "strikes": _safe_int(ps.get("strikes")),
                "h": _safe_int(ps.get("hits")),
                "r": _safe_int(ps.get("runs")),
                "er": _safe_int(ps.get("earnedRuns")),
                "bb": _safe_int(ps.get("baseOnBalls")),
                "k": _safe_int(ps.get("strikeOuts")),
                "hr": _safe_int(ps.get("homeRuns")),
                "hbp": _safe_int(ps.get("hitByPitch")),
                "ground_balls": _safe_int(ps.get("groundOuts")),
                "fly_balls": _safe_int(ps.get("airOuts")),
                "line_drives": None,  # MLB Stats doesn't break this out cleanly in boxscore
            },
        )


def upsert_team_game_log(
    engine: Engine,
    *,
    team_id: int,
    game_id: int,
    is_home: bool,
    opp_starter_throws: str | None,
    batting_stats: dict[str, Any],
    runs_allowed: int,
) -> None:
    """Upsert one team's batting line for a game."""
    bs = batting_stats
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.team_game_logs (
                    team_id, game_id, is_home, opp_starter_throws,
                    runs_scored, runs_allowed,
                    hits, doubles, triples, hr,
                    bb, k, hbp, sb, cs, lob
                )
                VALUES (
                    :team_id, :game_id, :is_home, :opp_starter_throws,
                    :runs_scored, :runs_allowed,
                    :hits, :doubles, :triples, :hr,
                    :bb, :k, :hbp, :sb, :cs, :lob
                )
                ON CONFLICT (team_id, game_id) DO UPDATE SET
                    is_home = EXCLUDED.is_home,
                    opp_starter_throws = EXCLUDED.opp_starter_throws,
                    runs_scored = EXCLUDED.runs_scored,
                    runs_allowed = EXCLUDED.runs_allowed,
                    hits = EXCLUDED.hits,
                    doubles = EXCLUDED.doubles,
                    triples = EXCLUDED.triples,
                    hr = EXCLUDED.hr,
                    bb = EXCLUDED.bb,
                    k = EXCLUDED.k,
                    hbp = EXCLUDED.hbp,
                    sb = EXCLUDED.sb,
                    cs = EXCLUDED.cs,
                    lob = EXCLUDED.lob
            """),
            {
                "team_id": team_id,
                "game_id": game_id,
                "is_home": is_home,
                "opp_starter_throws": opp_starter_throws,
                "runs_scored": _safe_int(bs.get("runs")),
                "runs_allowed": runs_allowed,
                "hits": _safe_int(bs.get("hits")),
                "doubles": _safe_int(bs.get("doubles")),
                "triples": _safe_int(bs.get("triples")),
                "hr": _safe_int(bs.get("homeRuns")),
                "bb": _safe_int(bs.get("baseOnBalls")),
                "k": _safe_int(bs.get("strikeOuts")),
                "hbp": _safe_int(bs.get("hitByPitch")),
                "sb": _safe_int(bs.get("stolenBases")),
                "cs": _safe_int(bs.get("caughtStealing")),
                "lob": _safe_int(bs.get("leftOnBase")),
            },
        )


def upsert_player_full(engine: Engine, person: dict[str, Any]) -> None:
    """Enrich an existing player stub with full /people data."""
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.players (
                    player_id, full_name, primary_position, throws, bats,
                    birth_date, mlb_debut_date
                )
                VALUES (
                    :player_id, :full_name, :primary_position, :throws, :bats,
                    :birth_date, :mlb_debut_date
                )
                ON CONFLICT (player_id) DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    primary_position = COALESCE(EXCLUDED.primary_position, {SCHEMA}.players.primary_position),
                    throws = COALESCE(EXCLUDED.throws, {SCHEMA}.players.throws),
                    bats = COALESCE(EXCLUDED.bats, {SCHEMA}.players.bats),
                    birth_date = COALESCE(EXCLUDED.birth_date, {SCHEMA}.players.birth_date),
                    mlb_debut_date = COALESCE(EXCLUDED.mlb_debut_date, {SCHEMA}.players.mlb_debut_date)
            """),
            {
                "player_id": person["id"],
                "full_name": person["fullName"],
                "primary_position": (person.get("primaryPosition") or {}).get("abbreviation"),
                "throws": (person.get("pitchHand") or {}).get("code"),
                "bats": (person.get("batSide") or {}).get("code"),
                "birth_date": person.get("birthDate"),
                "mlb_debut_date": person.get("mlbDebutDate"),
            },
        )


def upsert_player_stub(engine: Engine, player_id: int, full_name: str) -> None:
    """Insert a minimal player row if missing (FK requirement before pitcher game log)."""
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.players (player_id, full_name)
                VALUES (:player_id, :full_name)
                ON CONFLICT (player_id) DO UPDATE SET
                    full_name = COALESCE(NULLIF({SCHEMA}.players.full_name, ''), EXCLUDED.full_name)
            """),
            {"player_id": player_id, "full_name": full_name},
        )


# ---------------------------------------------------------------------------
# Orchestrators
# ---------------------------------------------------------------------------

def get_pitchers_needing_enrichment(engine: Engine, player_ids: list[int]) -> list[int]:
    """Return the subset of player_ids whose `throws` column is currently NULL."""
    if not player_ids:
        return []
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT player_id FROM {SCHEMA}.players
                WHERE player_id = ANY(:ids) AND throws IS NULL
            """),
            {"ids": player_ids},
        ).fetchall()
        return [r.player_id for r in rows]


def get_starter_throws(engine: Engine, player_id: int | None) -> str | None:
    if player_id is None:
        return None
    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT throws FROM {SCHEMA}.players WHERE player_id = :pid"),
            {"pid": player_id},
        ).fetchone()
        return row.throws if row else None


def sync_results_for_date(engine: Engine, target_date: str) -> dict[str, int]:
    """For target_date, ingest all Final games' outcomes + box scores."""
    client = MLBStatsClient()

    # Get all games on the date (including their status)
    schedule = client.get_schedule(target_date)
    logger.info("Fetched %d games for %s", len(schedule), target_date)

    final_games = [g for g in schedule if (g.get("status") or {}).get("abstractGameState") == "Final"]
    non_final = len(schedule) - len(final_games)
    if non_final:
        logger.info("Skipping %d non-final games on %s", non_final, target_date)

    outcomes_written = 0
    pitcher_logs_written = 0
    team_logs_written = 0
    pitchers_enriched = 0
    statuses_bumped = 0
    f5_computed = 0
    f5_skipped_short = 0
    pitcher_ids_seen: set[int] = set()

    for game in final_games:
        game_pk = game["gamePk"]
        try:
            box = client.get_box_score(game_pk)
            line = client.get_line_score(game_pk)

            # --- outcome ---
            home_score = game["teams"]["home"].get("score")
            away_score = game["teams"]["away"].get("score")
            if home_score is None or away_score is None:
                logger.warning("Game %s has Final status but missing scores — skipping", game_pk)
                continue

            scheduled_innings = _safe_int(line.get("scheduledInnings")) or 9
            current_inning = _safe_int(line.get("currentInning")) or scheduled_innings
            extra_innings = current_inning > scheduled_innings
            final_innings_val = float(current_inning)

            # First-5-innings actuals + raw per-inning linescore (stored regardless).
            innings = line.get("innings") or []
            home_f5, away_f5 = compute_f5_runs(innings)
            if home_f5 is None:
                logger.warning(
                    "Game %s: only %d inning(s) recorded (<5) — leaving F5 NULL",
                    game_pk, len(innings),
                )
                f5_skipped_short += 1
            else:
                f5_computed += 1

            upsert_outcome(
                engine,
                game_id=game_pk,
                home_score=home_score,
                away_score=away_score,
                extra_innings=extra_innings,
                final_innings=final_innings_val,
                home_runs_f5=home_f5,
                away_runs_f5=away_f5,
                linescore=innings,
            )
            outcomes_written += 1

            # Completed game — make sure games.status reflects that (downstream tools
            # otherwise see a stale In Progress/Warmup). Same tx-per-statement pattern.
            statuses_bumped += bump_game_status_to_final(engine, game_pk)

            # --- box score: pitchers + team batting per side ---
            for side in ("home", "away"):
                team_side = box["teams"][side]
                team_id = team_side["team"]["id"]
                is_home = (side == "home")
                runs_scored = home_score if is_home else away_score
                runs_allowed = away_score if is_home else home_score

                # Pitchers for this side
                pitcher_ids_in_order: list[int] = team_side.get("pitchers", [])
                players_block = team_side.get("players", {})

                for idx, pid in enumerate(pitcher_ids_in_order):
                    pitcher_ids_seen.add(pid)
                    player_entry = players_block.get(f"ID{pid}")
                    if not player_entry:
                        logger.warning("Game %s: pitcher id %s listed but not in players block", game_pk, pid)
                        continue

                    pitching_stats = (player_entry.get("stats") or {}).get("pitching") or {}
                    if not pitching_stats:
                        # Listed pitcher with no pitching stats — shouldn't happen but skip safely
                        continue

                    full_name = (player_entry.get("person") or {}).get("fullName") or f"Player {pid}"

                    # Stub the player row first (FK target)
                    upsert_player_stub(engine, pid, full_name)

                    is_starter = (idx == 0)
                    upsert_pitcher_game_log(
                        engine,
                        player_id=pid,
                        game_id=game_pk,
                        team_id=team_id,
                        is_starter=is_starter,
                        pitching_stats=pitching_stats,
                    )
                    pitcher_logs_written += 1

                # Team batting line
                batting_stats = (team_side.get("teamStats") or {}).get("batting") or {}

                # Opposing starter's handedness (denormalized for fast splits queries)
                opp_side = "away" if is_home else "home"
                opp_pitchers = box["teams"][opp_side].get("pitchers", [])
                opp_starter_id = opp_pitchers[0] if opp_pitchers else None
                opp_throws = get_starter_throws(engine, opp_starter_id)

                upsert_team_game_log(
                    engine,
                    team_id=team_id,
                    game_id=game_pk,
                    is_home=is_home,
                    opp_starter_throws=opp_throws,
                    batting_stats=batting_stats,
                    runs_allowed=runs_allowed,
                )
                team_logs_written += 1

        except Exception as e:
            logger.warning("Failed to ingest game %s: %s", game_pk, e)

    # Enrich players who appeared today and don't have throws/bats yet
    needs_enrich = get_pitchers_needing_enrichment(engine, list(pitcher_ids_seen))
    logger.info("%d pitchers need enrichment (throws/bats/etc missing)", len(needs_enrich))
    for pid in needs_enrich:
        try:
            person = client.get_person(pid)
            upsert_player_full(engine, person)
            pitchers_enriched += 1
        except Exception as e:
            logger.warning("Failed to enrich player %s: %s", pid, e)

    logger.info(
        "Results for %s — outcomes: %d, pitcher logs: %d, team logs: %d, "
        "statuses bumped to Final: %d, pitchers enriched: %d, "
        "F5 computed: %d (skipped short games: %d)",
        target_date, outcomes_written, pitcher_logs_written, team_logs_written,
        statuses_bumped, pitchers_enriched, f5_computed, f5_skipped_short,
    )

    return {
        "outcomes": outcomes_written,
        "pitcher_logs": pitcher_logs_written,
        "team_logs": team_logs_written,
        "pitchers_enriched": pitchers_enriched,
        "statuses_bumped": statuses_bumped,
        "f5_computed": f5_computed,
        "f5_skipped_short": f5_skipped_short,
        "games_skipped_non_final": non_final,
    }
