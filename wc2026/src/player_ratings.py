"""Display-only aggregation of per-player match ratings across recent fixtures.

Consumes raw fixture responses (the same ones the model fetches for a team)
and delegates per-fixture extraction to :func:`parse_fixture_player_ratings`.
Does not import or interact with prediction or team-aggregation logic.
"""

from __future__ import annotations

import json
import logging
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from src import config
from src.sportmonks_parser import parse_fixture_player_ratings
from src.sportmonks_client import SportmonksClient

__all__ = [
    "MIN_RATED_APPEARANCES",
    "PlayerRatingsResult",
    "aggregate_player_ratings",
    "build_matchup_player_ratings",
    "build_player_ratings_for_team",
    "fetch_lineup_fixtures_for_team",
    "match_rated_players_to_squad",
    "normalize_name_tokens",
    "squad_shared_dobs",
    "player_ratings_result_to_dict",
    "persist_team_ratings_snapshot",
    "snapshot_player_ratings_for_pool",
    "persist_projected_lineup_snapshot",
    "snapshot_projected_lineups_for_pool",
    "build_projected_xi",
    "build_team_player_display_block",
    "build_matchup_player_display",
    "resolve_team_code_for_id",
    "empty_team_player_display",
]

_EMPTY_TEAM_RATINGS: dict[str, list[Any]] = {
    "listed": [],
    "insufficient_data": [],
}

_SQUAD_DISPLAY_FIELDS: tuple[str, ...] = (
    "squad_no",
    "player_name",
    "position",
    "club",
    "club_country",
    "dob",
    "name_on_shirt",
    "height_cm",
)

logger = logging.getLogger(__name__)

MIN_RATED_APPEARANCES = 1


def normalize_name_tokens(name: str) -> set[str]:
    """Normalize a name to an uppercase A-Z token set (accent-stripped)."""
    decomposed = unicodedata.normalize("NFKD", name or "")
    stripped = "".join(
        ch for ch in decomposed if not unicodedata.combining(ch)
    )
    upper = stripped.upper()
    letters_only = "".join(
        ch if ("A" <= ch <= "Z") or ch == " " else " " for ch in upper
    )
    return {token for token in letters_only.split() if token}


def _squad_identity_tokens(squad_row: dict[str, Any]) -> set[str]:
    """Token union of FIFA last_names, name_on_shirt, and first_names."""
    tokens: set[str] = set()
    for field in ("last_names", "name_on_shirt", "first_names"):
        tokens |= normalize_name_tokens(str(squad_row.get(field, "")))
    return tokens


def _names_agree(sm_tokens: set[str], squad_tokens: set[str]) -> bool:
    if not sm_tokens or not squad_tokens:
        return False
    return (
        squad_tokens.issubset(sm_tokens) or sm_tokens.issubset(squad_tokens)
    )


@dataclass
class _PlayerAccumulator:
    player_name: str = "?"
    dob: str | None = None
    weighted_rating_sum: float = 0.0
    minutes_weight_sum: float = 0.0
    minutes_sum: float = 0.0
    fixture_ids: set[int] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class PlayerRatingsResult:
    """Ranked player ratings for one national team."""

    team_id: int
    listed: tuple[dict[str, Any], ...]
    insufficient_data: tuple[dict[str, Any], ...]


def aggregate_player_ratings(
    team_id: int,
    fixtures: list[dict[str, Any]],
) -> PlayerRatingsResult:
    """Average player RATINGs across recent fixtures, weighted by minutes played.

    Each fixture is parsed independently; cross-fixture merging happens only
    here, keyed by ``player_id`` and attributed via distinct ``fixture_id``
    values from the parser.

    Players with at least :data:`MIN_RATED_APPEARANCES` rated appearances are
    returned in ``listed`` (sorted by ``avg_rating`` descending). Players with
    fewer rated appearances are surfaced in ``insufficient_data`` with
    ``status="insufficient_data"`` rather than dropped silently.
    """
    accumulators: dict[int, _PlayerAccumulator] = {}

    for fixture in fixtures:
        for row in parse_fixture_player_ratings(fixture, team_id):
            player_id = int(row["player_id"])
            acc = accumulators.get(player_id)
            if acc is None:
                acc = _PlayerAccumulator()
                accumulators[player_id] = acc

            acc.player_name = str(row["player_name"])
            if acc.dob is None and row.get("dob"):
                acc.dob = str(row["dob"])
            minutes = float(row["minutes_played"])
            weight = minutes if minutes > 0 else 1.0
            acc.weighted_rating_sum += float(row["rating"]) * weight
            acc.minutes_weight_sum += weight
            acc.minutes_sum += max(minutes, 0.0)
            acc.fixture_ids.add(int(row["fixture_id"]))

    listed: list[dict[str, Any]] = []
    insufficient: list[dict[str, Any]] = []
    total_minutes = sum(acc.minutes_sum for acc in accumulators.values())

    for player_id, acc in accumulators.items():
        matches_counted = len(acc.fixture_ids)
        if acc.minutes_weight_sum <= 0:
            logger.warning(
                "Skipping player_id=%d on team_id=%d: zero weight across fixtures.",
                player_id,
                team_id,
            )
            continue

        avg_rating = acc.weighted_rating_sum / acc.minutes_weight_sum
        minutes_share = (
            acc.minutes_sum / total_minutes if total_minutes > 0 else None
        )
        entry: dict[str, Any] = {
            "player_id": player_id,
            "player_name": acc.player_name,
            "avg_rating": avg_rating,
            "matches_counted": matches_counted,
            "dob": acc.dob,
            "minutes_share": minutes_share,
            "source": "sportmonks",
        }

        if matches_counted >= MIN_RATED_APPEARANCES:
            listed.append(entry)
        else:
            insufficient.append({**entry, "status": "insufficient_data"})

    listed.sort(key=lambda row: row["avg_rating"], reverse=True)
    insufficient.sort(
        key=lambda row: (-row["matches_counted"], row["player_name"].lower()),
    )

    return PlayerRatingsResult(
        team_id=team_id,
        listed=tuple(listed),
        insufficient_data=tuple(insufficient),
    )


def player_ratings_result_to_dict(
    result: PlayerRatingsResult,
    *,
    source: str = "sportmonks",
    window_start_date: str | None = None,
    window_end_date: str | None = None,
) -> dict[str, Any]:
    """Serialize a :class:`PlayerRatingsResult` for JSON report output."""

    def _row(entry: dict[str, Any]) -> dict[str, Any]:
        serialized = dict(entry)
        serialized["avg_rating"] = round(float(serialized["avg_rating"]), 2)
        if serialized.get("minutes_share") is not None:
            serialized["minutes_share"] = round(float(serialized["minutes_share"]), 4)
        serialized.setdefault("source", source)
        return serialized

    return {
        "source": source,
        "window_start_date": window_start_date,
        "window_end_date": window_end_date,
        "listed": [_row(row) for row in result.listed],
        "insufficient_data": [_row(row) for row in result.insufficient_data],
    }


def _fixture_date_str(fixture: dict[str, Any]) -> str | None:
    for key in ("starting_at", "starting_at_date"):
        if fixture.get(key):
            return str(fixture[key])[:10]
    ts = fixture.get("starting_at_timestamp")
    if ts:
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")
    return None


def _fixture_window_dates(fixtures: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    dates = [_fixture_date_str(f) for f in fixtures]
    valid = [d for d in dates if d]
    if not valid:
        return None, None
    return min(valid), max(valid)


def fetch_lineup_fixtures_for_team(
    client: SportmonksClient,
    team_id: int,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch the team's most recent competitive fixtures with lineup player stats.

    Uses the same SportMonks date-ranged team endpoint as the model bootstrap
    (qualifiers + WC finals, league 732 included). As World Cup matches are
    played they displace older qualifiers in this window and player averages
    repopulate automatically — including for host nations that have no qualifier
    rows in the prediction pool.
    """
    window = limit if limit is not None else config.PLAYER_RATINGS_MATCH_WINDOW
    fixtures: list[dict[str, Any]] = []
    try:
        summaries = client.get_fixtures_for_team(team_id, limit=window)
    except Exception as exc:  # noqa: BLE001 - display-only; never block prediction
        logger.warning(
            "Could not list fixtures for team_id=%d player ratings: %s",
            team_id,
            exc,
        )
        return fixtures

    for summary in summaries:
        fixture_id = summary.get("id")
        if fixture_id is None:
            continue
        try:
            fixture = client.get_fixture_with_lineups(int(fixture_id))
            if fixture:
                fixtures.append(fixture)
        except Exception as exc:  # noqa: BLE001 - display-only; never block prediction
            logger.warning(
                "Could not fetch fixture %d for team_id=%d player ratings: %s",
                fixture_id,
                team_id,
                exc,
            )
    return fixtures


def _manual_team_id_for_code(team_code: str) -> int | None:
    """Resolve SportMonks team_id from data/manual_player_ratings/*.json."""
    manual_dir = Path(__file__).resolve().parent.parent / "data" / "manual_player_ratings"
    if not manual_dir.is_dir():
        return None
    for path in sorted(manual_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if str(payload.get("team_code", "")) == team_code:
            sm_id = payload.get("sportmonks_team_id")
            return int(sm_id) if sm_id is not None else None
    return None


def _manual_team_code_for_id(team_id: int) -> str | None:
    """Map SportMonks team_id -> FIFA code from data/manual_player_ratings/*.json."""
    manual_dir = Path(__file__).resolve().parent.parent / "data" / "manual_player_ratings"
    if not manual_dir.is_dir():
        return None
    for path in sorted(manual_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if int(payload.get("sportmonks_team_id", -1)) == team_id:
            return str(payload["team_code"])
    return None


_MANUAL_RATINGS_DIR = Path(__file__).resolve().parent.parent / "data" / "manual_player_ratings"
_SQUADS_CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "wc2026_squads.csv"


def _squad_shirt_map_from_csv(team_code: str) -> dict[str, int]:
    """Map FIFA name_on_shirt -> squad_no from the local roster CSV."""
    import csv

    if not _SQUADS_CSV_PATH.is_file():
        return {}
    mapping: dict[str, int] = {}
    with _SQUADS_CSV_PATH.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("team_code", "")).upper() != team_code.upper():
                continue
            shirt = str(row.get("name_on_shirt", "")).strip().upper()
            if shirt:
                mapping[shirt] = int(row["squad_no"])
    return mapping


def _squad_rows_for_code(team_code: str) -> list[dict[str, str]]:
    import csv

    if not _SQUADS_CSV_PATH.is_file():
        return []
    rows: list[dict[str, str]] = []
    with _SQUADS_CSV_PATH.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("team_code", "")).upper() == team_code.upper():
                rows.append(row)
    return rows


def _player_name_for_squad_csv(team_code: str, squad_no: int) -> str:
    import csv

    if not _SQUADS_CSV_PATH.is_file():
        return ""
    with _SQUADS_CSV_PATH.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if (
                str(row.get("team_code", "")).upper() == team_code.upper()
                and int(row["squad_no"]) == squad_no
            ):
                return str(row.get("player_name", ""))
    return ""


def _load_manual_ratings_from_json(team_code: str) -> dict[str, Any] | None:
    """Load hardcoded friendly ratings when a manual JSON file exists for the team."""
    path = _MANUAL_RATINGS_DIR / f"{team_code.upper()}.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read manual ratings %s: %s", path, exc)
        return None

    from collections import defaultdict

    shirt_map = _squad_shirt_map_from_csv(team_code)
    accum: dict[int, list[float]] = defaultdict(list)
    for fixture in payload.get("fixture_ratings", []):
        players = fixture.get("players", {})
        if not isinstance(players, dict):
            continue
        for shirt, rating in players.items():
            key = str(shirt).strip()
            squad_no: int | None = None
            if key.isdigit():
                squad_no = int(key)
            elif key.lower().startswith("squad:"):
                squad_no = int(key.split(":", 1)[1])
            else:
                squad_no = shirt_map.get(key.upper())
            if squad_no is None or squad_no not in {
                int(row["squad_no"]) for row in _squad_rows_for_code(team_code)
            }:
                logger.warning(
                    "Manual ratings: unknown player key %r for %s", shirt, team_code
                )
                continue
            accum[squad_no].append(float(rating))

    if not accum:
        return None

    listed: list[dict[str, Any]] = []
    insufficient: list[dict[str, Any]] = []
    for squad_no, ratings in sorted(accum.items()):
        matches_counted = len(ratings)
        avg_rating = round(sum(ratings) / matches_counted, 2)
        entry: dict[str, Any] = {
            "player_id": 0,
            "player_name": _player_name_for_squad_csv(team_code, squad_no),
            "avg_rating": avg_rating,
            "matches_counted": matches_counted,
            "dob": None,
            "squad_no": squad_no,
            "minutes_share": None,
        }
        if matches_counted >= MIN_RATED_APPEARANCES:
            listed.append(entry)
        else:
            insufficient.append({**entry, "status": "insufficient_data"})

    listed.sort(key=lambda row: row["avg_rating"], reverse=True)
    insufficient.sort(key=lambda row: (-row["matches_counted"], row["player_name"]))
    return {
        "source": "manual",
        "listed": listed,
        "insufficient_data": insufficient,
        "window_start_date": None,
        "window_end_date": None,
    }


def _fallback_team_player_ratings(team_id: int) -> dict[str, Any] | None:
    """Load persisted manual averages when SportMonks has no per-player ratings."""
    try:
        from src.database import (
            DatabaseError,
            get_fifa_code_for_team_id,
            get_team_player_ratings_for_display,
        )
    except ImportError:
        return None

    try:
        team_code = get_fifa_code_for_team_id(team_id) or _manual_team_code_for_id(
            team_id
        )
        if not team_code:
            return None
        return get_team_player_ratings_for_display(
            team_code,
            min_appearances=MIN_RATED_APPEARANCES,
        )
    except DatabaseError as exc:
        logger.warning(
            "Could not load fallback player ratings for team_id=%d: %s",
            team_id,
            exc,
        )
        return None


def build_player_ratings_for_team(
    client: SportmonksClient,
    team_id: int,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    """Fetch recent fixtures with lineups and aggregate display ratings.

    Always sources fixtures live from SportMonks (not the cached prediction
    pool) so ratings refresh as new matches are played. When the API returns no
    rated players (e.g. OFC qualifiers), falls back to Neon team_player_ratings.
    Fails soft.
    """
    try:
        from src.database import get_fifa_code_for_team_id
    except ImportError:
        team_code = _manual_team_code_for_id(team_id)
    else:
        team_code = get_fifa_code_for_team_id(team_id) or _manual_team_code_for_id(
            team_id
        )
    if team_code:
        manual = _load_manual_ratings_from_json(team_code)
        if manual is not None:
            return manual

    fixtures = fetch_lineup_fixtures_for_team(client, team_id, limit=limit)

    if fixtures:
        try:
            window_start, window_end = _fixture_window_dates(fixtures)
            result = player_ratings_result_to_dict(
                aggregate_player_ratings(team_id, fixtures),
                source="sportmonks",
                window_start_date=window_start,
                window_end_date=window_end,
            )
            if result["listed"] or result["insufficient_data"]:
                return result
        except Exception as exc:  # noqa: BLE001 - display-only; never block prediction
            logger.warning(
                "Failed to aggregate player ratings for team_id=%d: %s",
                team_id,
                exc,
            )
    else:
        logger.warning(
            "No fixtures with lineups for team_id=%d; checking manual ratings.",
            team_id,
        )

    fallback = _fallback_team_player_ratings(team_id)
    if fallback:
        logger.info(
            "Using persisted team_player_ratings for team_id=%d (%d listed, "
            "%d insufficient)",
            team_id,
            len(fallback["listed"]),
            len(fallback["insufficient_data"]),
        )
        return fallback

    empty = dict(_EMPTY_TEAM_RATINGS)
    empty["source"] = "none"
    empty["window_start_date"] = None
    empty["window_end_date"] = None
    return empty


def persist_team_ratings_snapshot(
    team_code: str,
    team_ratings: dict[str, Any],
    *,
    snapshot_date: date | None = None,
) -> int:
    """UPSERT one daily history row per rated player. Fail-soft on DB errors."""
    try:
        from src.database import DatabaseError, upsert_player_ratings_history_rows
    except ImportError:
        return 0

    source = str(team_ratings.get("source") or "sportmonks")
    if source in ("none", ""):
        return 0

    players = list(team_ratings.get("listed", [])) + list(
        team_ratings.get("insufficient_data", [])
    )
    if not players:
        return 0

    snap = snapshot_date or date.today()
    window_start = team_ratings.get("window_start_date")
    window_end = team_ratings.get("window_end_date")
    db_rows: list[dict[str, Any]] = []

    for player in players:
        if source == "manual":
            squad_no = player.get("squad_no")
            if squad_no is None:
                continue
            entity_key = f"manual:{team_code}:{int(squad_no)}"
            db_rows.append(
                {
                    "entity_key": entity_key,
                    "sportmonks_player_id": None,
                    "team_code": team_code,
                    "manual_squad_no": int(squad_no),
                    "player_name": str(player["player_name"]),
                    "avg_rating": float(player["avg_rating"]),
                    "minutes_share": None,
                    "matches_counted": int(player["matches_counted"]),
                    "source": "manual",
                    "window_start_date": None,
                    "window_end_date": None,
                    "snapshot_date": snap,
                }
            )
        else:
            sm_id = int(player["player_id"])
            db_rows.append(
                {
                    "entity_key": f"sm:{sm_id}",
                    "sportmonks_player_id": sm_id,
                    "team_code": team_code,
                    "manual_squad_no": None,
                    "player_name": str(player["player_name"]),
                    "avg_rating": float(player["avg_rating"]),
                    "minutes_share": player.get("minutes_share"),
                    "matches_counted": int(player["matches_counted"]),
                    "source": "sportmonks",
                    "window_start_date": window_start,
                    "window_end_date": window_end,
                    "snapshot_date": snap,
                }
            )

    if not db_rows:
        return 0

    try:
        return upsert_player_ratings_history_rows(db_rows)
    except DatabaseError as exc:
        logger.warning(
            "Could not persist player_ratings_history for %s: %s",
            team_code,
            exc,
        )
        return 0


def snapshot_player_ratings_for_pool(pool: Any) -> dict[str, Any]:
    """Compute and persist one ratings snapshot per WC squad team. Fail-soft.

    Uses the 48 ``wc2026_squads`` team codes (not the full bootstrap roster).
    ``pool`` is accepted for cron signature compatibility but not iterated.
    """
    del pool  # WC squad list is the authoritative team universe for snapshots.
    try:
        from src.database import (
            get_team_id_for_fifa_code,
            get_wc2026_squad_team_codes,
        )
    except ImportError:
        return {
            "teams_written": 0,
            "rows_written": 0,
            "by_source": {},
            "skipped_team_codes": [],
        }

    client = SportmonksClient()
    by_source: dict[str, int] = {}
    teams_written = 0
    rows_written = 0
    skipped: list[str] = []

    for team_code in get_wc2026_squad_team_codes():
        team_id = get_team_id_for_fifa_code(team_code) or _manual_team_id_for_code(
            team_code
        )
        if team_id is None:
            skipped.append(team_code)
            continue

        ratings = build_player_ratings_for_team(client, team_id)
        source = str(ratings.get("source") or "none")
        n = persist_team_ratings_snapshot(team_code, ratings)
        if n > 0:
            teams_written += 1
            rows_written += n
            by_source[source] = by_source.get(source, 0) + 1

    return {
        "teams_written": teams_written,
        "rows_written": rows_written,
        "by_source": by_source,
        "skipped_team_codes": skipped,
    }


def _flatten_projected_lineup_rows(
    team_code: str,
    display: dict[str, Any],
    *,
    snapshot_date: date | None = None,
) -> list[dict[str, Any]]:
    """Turn one team display block into UPSERT rows for projected_lineups_history."""
    snap = snapshot_date or date.today()
    team_xi_status = str(display.get("status") or "no_qualifier_data")
    ratings_source = display.get("ratings_source")
    rows: list[dict[str, Any]] = []

    for role, players in (
        ("projected_xi", display.get("projected_xi") or []),
        ("bench", display.get("bench") or []),
    ):
        for slot, player in enumerate(players, start=1):
            squad_no = player.get("squad_no")
            if squad_no is None:
                continue
            rows.append(
                {
                    "team_code": team_code.upper(),
                    "snapshot_date": snap,
                    "lineup_role": role,
                    "lineup_slot": slot,
                    "squad_no": int(squad_no),
                    "sportmonks_player_id": _valid_sm_player_id(
                        player.get("sportmonks_player_id")
                    ),
                    "player_name": str(player.get("player_name", "")),
                    "position": player.get("position"),
                    "avg_rating": player.get("avg_rating"),
                    "minutes_share": player.get("minutes_share"),
                    "matches_counted": player.get("matches_counted"),
                    "match_method": player.get("match_method"),
                    "team_xi_status": team_xi_status,
                    "ratings_source": ratings_source,
                }
            )
    return rows


def persist_projected_lineup_snapshot(
    team_code: str,
    display: dict[str, Any],
    *,
    snapshot_date: date | None = None,
) -> int:
    """UPSERT projected XI + bench rows for one team. Fail-soft on DB errors."""
    rows = _flatten_projected_lineup_rows(
        team_code, display, snapshot_date=snapshot_date
    )
    if not rows:
        return 0
    try:
        from src.database import DatabaseError, upsert_projected_lineups_history_rows
    except ImportError:
        return 0
    try:
        return upsert_projected_lineups_history_rows(rows)
    except DatabaseError as exc:
        logger.warning(
            "Could not persist projected_lineups_history for %s: %s",
            team_code,
            exc,
        )
        return 0


def snapshot_projected_lineups_for_pool(pool: Any) -> dict[str, Any]:
    """Compute and persist one projected-lineup snapshot per WC squad team."""
    del pool
    try:
        from src.database import (
            get_team_id_for_fifa_code,
            get_wc2026_squad_team_codes,
        )
    except ImportError:
        return {
            "teams_written": 0,
            "rows_written": 0,
            "xi_ok_teams": 0,
            "skipped_team_codes": [],
        }

    client = SportmonksClient()
    teams_written = 0
    rows_written = 0
    xi_ok_teams = 0
    skipped: list[str] = []

    for team_code in get_wc2026_squad_team_codes():
        team_id = get_team_id_for_fifa_code(team_code) or _manual_team_id_for_code(
            team_code
        )
        if team_id is None:
            skipped.append(team_code)
            continue

        display = build_team_player_display_for_code(team_code, team_id, client)
        n = persist_projected_lineup_snapshot(team_code, display)
        if n > 0:
            teams_written += 1
            rows_written += n
            if display.get("status") == "ok" and len(display.get("projected_xi") or []) == 11:
                xi_ok_teams += 1

    return {
        "teams_written": teams_written,
        "rows_written": rows_written,
        "xi_ok_teams": xi_ok_teams,
        "skipped_team_codes": skipped,
    }


@dataclass(frozen=True, slots=True)
class SquadMatchResult:
    """One SportMonks rated player matched (or not) to a FIFA squad row."""

    sportmonks_player_id: int
    sportmonks_name: str
    sportmonks_dob: str | None
    squad_no: str | None
    squad_player_name: str | None
    squad_dob: str | None
    method: str
    confidence: float
    flagged: bool


def squad_shared_dobs(squad_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Return squad rows grouped by DOB where more than one player shares a date."""
    by_dob: dict[str, list[dict[str, Any]]] = {}
    for row in squad_rows:
        dob = str(row.get("dob", "")).strip()
        if dob:
            by_dob.setdefault(dob, []).append(row)
    return {dob: rows for dob, rows in by_dob.items() if len(rows) > 1}


def _finalize_dob_match(
    row: dict[str, Any],
    sm_tokens: set[str],
) -> tuple[dict[str, Any], str, float, bool]:
    """Classify a unique DOB hit; name agreement is confidence-only."""
    if _names_agree(sm_tokens, _squad_identity_tokens(row)):
        return row, "dob+name", 1.0, False
    return row, "dob_only", 0.8, True


def match_rated_players_to_squad(
    rated_players: list[dict[str, Any]],
    squad_rows: list[dict[str, Any]],
) -> tuple[list[SquadMatchResult], list[dict[str, Any]]]:
    """Match aggregated SportMonks players to FIFA squad rows for one team.

    DOB is the only match anchor within a team. Name tokens (last_names,
    name_on_shirt, first_names) adjust confidence but never match without DOB.
    Returns ``(matches, unmatched_squad_rows)``.
    """
    matched_squad_nos: set[str] = set()
    results: list[SquadMatchResult] = []

    for player in rated_players:
        player_id = int(player["player_id"])
        sm_name = str(player.get("player_name", "?"))
        sm_dob = player.get("dob")
        sm_tokens = normalize_name_tokens(sm_name)

        pool = [
            row
            for row in squad_rows
            if str(row.get("squad_no", "")) not in matched_squad_nos
        ]

        chosen: dict[str, Any] | None = None
        method = "unmatched_ambiguous"
        confidence = 0.0
        flagged = False

        if not sm_dob:
            method = "no_dob"
        else:
            dob_pool = [row for row in pool if row.get("dob") == sm_dob]

            if len(dob_pool) == 1:
                chosen, method, confidence, flagged = _finalize_dob_match(
                    dob_pool[0], sm_tokens
                )
            elif len(dob_pool) > 1:
                name_hits = [
                    row
                    for row in dob_pool
                    if _names_agree(sm_tokens, _squad_identity_tokens(row))
                ]
                if len(name_hits) == 1:
                    chosen, method, confidence, flagged = _finalize_dob_match(
                        name_hits[0], sm_tokens
                    )
                else:
                    candidates = ", ".join(
                        str(row.get("player_name", "?")) for row in dob_pool
                    )
                    logger.warning(
                        "Ambiguous DOB match for %r (dob=%s): %d squad row(s) "
                        "share DOB [%s]; name hits=%d.",
                        sm_name,
                        sm_dob,
                        len(dob_pool),
                        candidates,
                        len(name_hits),
                    )
                    method = "unmatched_ambiguous"
            else:
                method = "not_in_current_squad"

        if chosen is not None:
            squad_no = str(chosen.get("squad_no", ""))
            matched_squad_nos.add(squad_no)
            results.append(
                SquadMatchResult(
                    sportmonks_player_id=player_id,
                    sportmonks_name=sm_name,
                    sportmonks_dob=sm_dob,
                    squad_no=squad_no,
                    squad_player_name=str(chosen.get("player_name", "")),
                    squad_dob=str(chosen.get("dob", "")) or None,
                    method=method,
                    confidence=confidence,
                    flagged=flagged,
                )
            )
        else:
            results.append(
                SquadMatchResult(
                    sportmonks_player_id=player_id,
                    sportmonks_name=sm_name,
                    sportmonks_dob=sm_dob,
                    squad_no=None,
                    squad_player_name=None,
                    squad_dob=None,
                    method=method,
                    confidence=0.0,
                    flagged=False,
                )
            )

    unmatched_squad = [
        row
        for row in squad_rows
        if str(row.get("squad_no", "")) not in matched_squad_nos
    ]
    return results, unmatched_squad


_POSITION_BUCKETS: tuple[str, ...] = ("GK", "DF", "MF", "FW")
_DEFAULT_FORMATION: tuple[int, int, int, int] = (1, 4, 3, 3)


def _normalize_id_map(id_map: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """Index id_map rows by sportmonks_player_id."""
    lookup: dict[int, dict[str, Any]] = {}
    for row in id_map:
        player_id = row.get("sportmonks_player_id")
        if player_id is None:
            continue
        lookup[int(player_id)] = row
    return lookup


def _valid_sm_player_id(value: Any) -> int | None:
    """Return a positive SportMonks player id, or None (0 is a manual-ratings sentinel)."""
    if value is None:
        return None
    try:
        sm_id = int(value)
    except (TypeError, ValueError):
        return None
    return sm_id if sm_id > 0 else None


def _squad_no_to_sm_id(id_map: list[dict[str, Any]]) -> dict[int, int]:
    """Reverse index: FIFA squad_no -> sportmonks_player_id for one team."""
    lookup: dict[int, int] = {}
    for row in id_map:
        squad_no = row.get("squad_no")
        player_id = row.get("sportmonks_player_id")
        if squad_no is None or player_id is None:
            continue
        lookup[int(squad_no)] = int(player_id)
    return lookup


def _load_manual_sm_id_overrides() -> dict[str, dict[int, int]]:
    """team_code -> squad_no -> sportmonks_player_id from manual_player_id_overrides.json."""
    if not _MANUAL_PLAYER_ID_OVERRIDES_PATH.is_file():
        return {}
    try:
        payload = json.loads(
            _MANUAL_PLAYER_ID_OVERRIDES_PATH.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read manual_player_id_overrides.json: %s", exc)
        return {}
    by_team: dict[str, dict[int, int]] = {}
    for entry in payload.get("overrides", []):
        if not isinstance(entry, dict):
            continue
        code = str(entry.get("team_code", "")).upper()
        squad_no = entry.get("squad_no")
        sm_id = entry.get("sportmonks_player_id")
        if not code or squad_no is None or sm_id is None:
            continue
        by_team.setdefault(code, {})[int(squad_no)] = int(sm_id)
    return by_team


def _lineup_squad_to_sm_id(
    team_code: str | None,
    id_map: list[dict[str, Any]],
) -> dict[int, int]:
    """Merged squad_no -> sportmonks_player_id (Neon id_map + manual overrides)."""
    lookup = _squad_no_to_sm_id(id_map)
    if team_code:
        for squad_no, sm_id in _load_manual_sm_id_overrides().get(
            team_code.upper(), {}
        ).items():
            lookup[squad_no] = sm_id
    return lookup


def _squad_row_search_names(squad_row: dict[str, Any]) -> list[str]:
    """Build SportMonks search queries from FIFA squad name fields."""
    first = str(squad_row.get("first_names", "")).strip()
    last = str(squad_row.get("last_names", "")).strip()
    shirt = str(squad_row.get("name_on_shirt", "")).strip()
    display = str(squad_row.get("player_name", "")).strip()
    variants: list[str] = []
    if first and last:
        variants.append(f"{first} {last}")
    if display:
        parts = display.split(None, 1)
        if len(parts) == 2:
            variants.append(f"{parts[1]} {parts[0].title()}")
        variants.append(display)
    if shirt:
        variants.append(shirt)
    if last:
        variants.append(last)
    seen: set[str] = set()
    ordered: list[str] = []
    for name in variants:
        key = name.upper()
        if name and key not in seen:
            seen.add(key)
            ordered.append(name)
    return ordered


def _squad_name_tokens(squad_row: dict[str, Any]) -> set[str]:
    return set(
        normalize_name_tokens(
            f"{squad_row.get('last_names', '')} "
            f"{squad_row.get('first_names', '')} "
            f"{squad_row.get('player_name', '')} "
            f"{squad_row.get('name_on_shirt', '')}"
        )
    )


def _parse_iso_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if hasattr(value, "isoformat"):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _team_roster_entries(
    team_id: int,
    client: SportmonksClient,
) -> list[dict[str, Any]]:
    """Cached SportMonks national-team roster (players.player include)."""
    if team_id in _team_roster_cache:
        return _team_roster_cache[team_id]
    try:
        payload = client.get(
            f"teams/{team_id}",
            params={"include": "players.player"},
        )
        raw = (payload.get("data") or {}).get("players") or []
    except Exception as exc:  # noqa: BLE001 - display-only fallback
        logger.debug("Could not load team roster for team_id=%d: %s", team_id, exc)
        _team_roster_cache[team_id] = []
        return []

    parsed: list[dict[str, Any]] = []
    for entry in raw:
        player = entry.get("player") or {}
        sm_id = _valid_sm_player_id(player.get("id"))
        if sm_id is None:
            continue
        parsed.append(
            {
                "id": sm_id,
                "name": str(player.get("display_name") or player.get("name") or ""),
                "dob": _parse_iso_date(player.get("date_of_birth")),
                "jersey_number": entry.get("jersey_number"),
            }
        )
    _team_roster_cache[team_id] = parsed
    return parsed


def _resolve_sm_id_from_team_roster(
    squad_row: dict[str, Any],
    team_id: int,
    *,
    client: SportmonksClient,
) -> int | None:
    """Match FIFA squad row to SportMonks national-team roster by DOB/name/jersey."""
    squad_no = int(squad_row["squad_no"])
    squad_dob = _parse_iso_date(squad_row.get("dob"))
    squad_tokens = _squad_name_tokens(squad_row)
    if not squad_tokens:
        return None

    jersey_hit: int | None = None
    exact_hit: int | None = None
    fuzzy_hit: int | None = None

    for entry in _team_roster_entries(team_id, client):
        name_tokens = set(normalize_name_tokens(entry["name"]))
        if not (squad_tokens & name_tokens):
            continue
        entry_dob = entry.get("dob")
        if (
            entry.get("jersey_number") is not None
            and int(entry["jersey_number"]) == squad_no
        ):
            jersey_hit = int(entry["id"])
        if squad_dob is not None and entry_dob == squad_dob:
            exact_hit = int(entry["id"])
        elif (
            squad_dob is not None
            and entry_dob is not None
            and abs((entry_dob - squad_dob).days) <= 7
        ):
            fuzzy_hit = int(entry["id"])

    return jersey_hit or exact_hit or fuzzy_hit


def _resolve_sm_id_for_squad_row(
    squad_row: dict[str, Any],
    *,
    client: SportmonksClient,
    team_id: int | None = None,
) -> int | None:
    """Fallback: SportMonks player search by squad name variants + DOB."""
    dob = squad_row.get("dob")
    dob_s = dob.isoformat() if hasattr(dob, "isoformat") else str(dob or "")
    for search_name in _squad_row_search_names(squad_row):
        cache_key = (search_name.upper(), dob_s)
        if cache_key in _sm_id_search_cache:
            cached = _sm_id_search_cache[cache_key]
            if cached is not None:
                return cached
            continue
        try:
            response = client.get(f"players/search/{search_name}")
        except Exception as exc:  # noqa: BLE001 - display-only fallback
            logger.debug(
                "SportMonks player search failed for %r: %s", search_name, exc
            )
            _sm_id_search_cache[cache_key] = None
            continue

        squad_dob = _parse_iso_date(dob_s) if dob_s else None
        for player in response.get("data") or []:
            player_dob = _parse_iso_date(player.get("date_of_birth"))
            if squad_dob is not None and player_dob is not None:
                if player_dob != squad_dob and abs((player_dob - squad_dob).days) > 7:
                    continue
            elif dob_s and str(player.get("date_of_birth")) != dob_s:
                continue
            sm_id = int(player["id"])
            _sm_id_search_cache[cache_key] = sm_id
            return sm_id
        _sm_id_search_cache[cache_key] = None

    if team_id is not None:
        return _resolve_sm_id_from_team_roster(
            squad_row, team_id, client=client
        )
    return None


def _enrich_missing_lineup_sm_ids(
    rows: list[dict[str, Any]],
    squad_rows: list[dict[str, Any]],
    squad_to_sm: dict[int, int],
    *,
    client: SportmonksClient | None = None,
    team_id: int | None = None,
) -> list[dict[str, Any]]:
    """Fill sportmonks_player_id on lineup rows using id_map, then name search."""
    if not rows:
        return rows
    squad_by_no = {int(row["squad_no"]): row for row in squad_rows}
    enriched: list[dict[str, Any]] = []
    for row in rows:
        out = dict(row)
        if _valid_sm_player_id(out.get("sportmonks_player_id")) is None:
            out.pop("sportmonks_player_id", None)
            squad_no = out.get("squad_no")
            if squad_no is not None:
                sm_id = squad_to_sm.get(int(squad_no))
                if sm_id is None and client is not None:
                    squad_row = squad_by_no.get(int(squad_no))
                    if squad_row is not None:
                        sm_id = _resolve_sm_id_for_squad_row(
                            squad_row,
                            client=client,
                            team_id=team_id,
                        )
                if sm_id is not None:
                    out["sportmonks_player_id"] = sm_id
        enriched.append(out)
    return enriched


def _attach_sm_ids_to_lineup_rows(
    rows: list[dict[str, Any]],
    squad_to_sm: dict[int, int],
) -> list[dict[str, Any]]:
    """Ensure each lineup row carries sportmonks_player_id when id_map has it."""
    enriched: list[dict[str, Any]] = []
    for row in rows:
        out = dict(row)
        if _valid_sm_player_id(out.get("sportmonks_player_id")) is None:
            out.pop("sportmonks_player_id", None)
            squad_no = out.get("squad_no")
            if squad_no is not None:
                sm_id = squad_to_sm.get(int(squad_no))
                if sm_id is not None:
                    out["sportmonks_player_id"] = sm_id
        enriched.append(out)
    return enriched


def _rated_players_from_team_ratings(team_ratings: dict[str, Any]) -> list[dict[str, Any]]:
    return list(team_ratings.get("listed", [])) + list(
        team_ratings.get("insufficient_data", [])
    )


def _xi_player_row(
    squad_row: dict[str, Any],
    rating_row: dict[str, Any],
    mapping_row: dict[str, Any],
) -> dict[str, Any]:
    minutes_share = rating_row.get("minutes_share")
    sm_id = _valid_sm_player_id(mapping_row.get("sportmonks_player_id"))
    if sm_id is None:
        sm_id = _valid_sm_player_id(rating_row.get("player_id"))
    row: dict[str, Any] = {
        "squad_no": int(squad_row["squad_no"]),
        "player_name": str(squad_row.get("player_name", "")),
        "position": str(squad_row.get("position", "")),
        "avg_rating": rating_row["avg_rating"],
        "minutes_share": minutes_share,
        "matches_counted": int(rating_row["matches_counted"]),
        "match_method": str(
            mapping_row.get("match_method", mapping_row.get("method", ""))
        ),
    }
    if sm_id is not None:
        row["sportmonks_player_id"] = sm_id
    return row


def build_projected_xi(
    team_ratings: dict[str, Any],
    squad_rows: list[dict[str, Any]],
    id_map: list[dict[str, Any]],
    formation: tuple[int, int, int, int] = _DEFAULT_FORMATION,
    *,
    team_code: str | None = None,
) -> dict[str, Any]:
    """Build a projected starting XI from ratings, squad rows, and id_map.

    Pure function: no network, no database. Rank within FIFA position buckets
    by ``minutes_share`` descending. Players must appear in ``id_map`` to be
    eligible.
    """
    empty: dict[str, Any] = {
        "projected_xi": [],
        "bench": [],
        "status": "no_qualifier_data",
    }

    if len(formation) != len(_POSITION_BUCKETS):
        raise ValueError(
            f"formation must have {len(_POSITION_BUCKETS)} slots "
            f"(GK, DF, MF, FW); got {len(formation)}"
        )

    id_lookup = _normalize_id_map(id_map)
    squad_to_sm = _lineup_squad_to_sm_id(team_code, id_map)
    squad_by_no = {int(row["squad_no"]): row for row in squad_rows}
    team_overrides = (
        _load_xi_overrides().get(team_code.upper(), {}) if team_code else {}
    )
    full_xi = team_overrides.get("full_xi") or []

    rated_players = _rated_players_from_team_ratings(team_ratings)
    if not rated_players and not full_xi:
        return dict(empty)

    matched: list[dict[str, Any]] = []
    seen_squad_nos: set[int] = set()
    for rating_row in rated_players:
        direct_squad_no = rating_row.get("squad_no")
        if direct_squad_no is not None:
            squad_no = int(direct_squad_no)
            if squad_no in seen_squad_nos:
                continue
            squad_row = squad_by_no.get(squad_no)
            if squad_row is None:
                continue
            manual_row: dict[str, Any] = {
                "squad_no": squad_no,
                "player_name": str(squad_row.get("player_name", "")),
                "position": str(squad_row.get("position", "")),
                "avg_rating": rating_row.get("avg_rating"),
                "minutes_share": rating_row.get("minutes_share"),
                "matches_counted": int(rating_row.get("matches_counted", 0)),
                "match_method": str(rating_row.get("source", "manual")),
            }
            sm_id = _valid_sm_player_id(rating_row.get("player_id"))
            if sm_id is None:
                sm_id = squad_to_sm.get(squad_no)
            if sm_id is not None:
                manual_row["sportmonks_player_id"] = sm_id
            matched.append(manual_row)
            seen_squad_nos.add(squad_no)
            continue

        player_id = int(rating_row["player_id"])
        mapping = id_lookup.get(player_id)
        if mapping is None:
            continue
        squad_no = int(mapping["squad_no"])
        if squad_no in seen_squad_nos:
            continue
        squad_row = squad_by_no.get(squad_no)
        if squad_row is None:
            continue
        matched.append(_xi_player_row(squad_row, rating_row, mapping))
        seen_squad_nos.add(squad_no)

    if full_xi:
        projected_xi, bench = _build_full_xi_from_override(
            full_xi, matched, squad_by_no
        )
        status = "ok" if len(projected_xi) == 11 else "partial"
        return {
            "projected_xi": _attach_sm_ids_to_lineup_rows(projected_xi, squad_to_sm),
            "bench": _attach_sm_ids_to_lineup_rows(bench, squad_to_sm),
            "status": status,
        }

    if not matched:
        return dict(empty)

    slot_counts = dict(zip(_POSITION_BUCKETS, formation, strict=True))
    buckets: dict[str, list[dict[str, Any]]] = {pos: [] for pos in _POSITION_BUCKETS}
    for row in matched:
        position = str(row["position"]).upper()
        if position in buckets:
            buckets[position].append(row)

    for position in _POSITION_BUCKETS:
        buckets[position].sort(
            key=lambda row: (
                row.get("minutes_share") is None,
                -(float(row["minutes_share"]) if row.get("minutes_share") is not None else 0.0),
                -float(row["avg_rating"]),
            )
        )

    projected_xi: list[dict[str, Any]] = []
    for position in _POSITION_BUCKETS:
        need = slot_counts[position]
        projected_xi.extend(buckets[position][:need])

    selected_nos = {int(row["squad_no"]) for row in projected_xi}
    bench = [row for row in matched if int(row["squad_no"]) not in selected_nos]
    bench.sort(
        key=lambda row: (
            row.get("minutes_share") is None,
            -(float(row["minutes_share"]) if row.get("minutes_share") is not None else 0.0),
            str(row["player_name"]).lower(),
        )
    )

    projected_xi, bench = _apply_xi_overrides(
        projected_xi,
        bench,
        matched,
        squad_by_no=squad_by_no,
        team_code=team_code,
    )

    template_filled = all(
        len([row for row in projected_xi if str(row["position"]).upper() == position])
        == slot_counts[position]
        for position in _POSITION_BUCKETS
    )
    status = "ok" if template_filled else "partial"

    return {
        "projected_xi": _attach_sm_ids_to_lineup_rows(projected_xi, squad_to_sm),
        "bench": _attach_sm_ids_to_lineup_rows(bench, squad_to_sm),
        "status": status,
    }


_XI_OVERRIDES_PATH = Path(__file__).resolve().parent.parent / "data" / "xi_overrides.json"
_MANUAL_PLAYER_ID_OVERRIDES_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "manual_player_id_overrides.json"
)
_sm_id_search_cache: dict[tuple[str, str], int | None] = {}
_team_roster_cache: dict[int, list[dict[str, Any]]] = {}


def _load_xi_overrides() -> dict[str, Any]:
    if not _XI_OVERRIDES_PATH.is_file():
        return {}
    try:
        payload = json.loads(_XI_OVERRIDES_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read xi_overrides.json: %s", exc)
        return {}
    teams = payload.get("teams", {})
    if not isinstance(teams, dict):
        return {}
    return {
        str(code).upper(): entry
        for code, entry in teams.items()
        if not str(code).startswith("_")
    }


def _xi_row_from_squad(
    squad_row: dict[str, Any],
    *,
    rating_row: dict[str, Any] | None = None,
    match_method: str = "manual_override",
) -> dict[str, Any]:
    squad_no = int(squad_row["squad_no"])
    if rating_row is not None:
        return {
            "squad_no": squad_no,
            "player_name": str(squad_row.get("player_name", "")),
            "position": str(squad_row.get("position", "")),
            "avg_rating": rating_row.get("avg_rating"),
            "minutes_share": rating_row.get("minutes_share"),
            "matches_counted": int(rating_row.get("matches_counted", 0)),
            "match_method": match_method,
        }
    return {
        "squad_no": squad_no,
        "player_name": str(squad_row.get("player_name", "")),
        "position": str(squad_row.get("position", "")),
        "avg_rating": None,
        "minutes_share": None,
        "matches_counted": 0,
        "match_method": match_method,
    }


def _build_full_xi_from_override(
    full_xi: list[int | str],
    matched: list[dict[str, Any]],
    squad_by_no: dict[int, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_no = {int(row["squad_no"]): row for row in matched}
    projected_xi: list[dict[str, Any]] = []
    for squad_no in full_xi:
        squad_no = int(squad_no)
        squad_row = squad_by_no.get(squad_no)
        if squad_row is None:
            continue
        rating_row = by_no.get(squad_no)
        projected_xi.append(
            _xi_row_from_squad(
                squad_row,
                rating_row=rating_row,
                match_method="manual_full_xi",
            )
        )
    selected_nos = {int(row["squad_no"]) for row in projected_xi}
    bench = [row for row in matched if int(row["squad_no"]) not in selected_nos]
    bench.sort(
        key=lambda row: (
            row.get("avg_rating") is None,
            -(float(row["avg_rating"]) if row.get("avg_rating") is not None else 0.0),
            str(row["player_name"]).lower(),
        )
    )
    return projected_xi, bench


def _apply_xi_overrides(
    projected_xi: list[dict[str, Any]],
    bench: list[dict[str, Any]],
    matched: list[dict[str, Any]],
    *,
    squad_by_no: dict[int, dict[str, Any]],
    team_code: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Swap manual starters into the XI (see data/xi_overrides.json)."""
    if not team_code:
        return projected_xi, bench

    team_overrides = _load_xi_overrides().get(team_code.upper(), {})
    if team_overrides.get("full_xi"):
        return projected_xi, bench

    force_starters = team_overrides.get("force_starters") or []
    if not force_starters:
        return projected_xi, bench

    xi = list(projected_xi)
    xi_nos = {int(row["squad_no"]) for row in xi}
    by_no = {int(row["squad_no"]): row for row in matched}

    for squad_no in force_starters:
        squad_no = int(squad_no)
        if squad_no in xi_nos:
            continue
        player = by_no.get(squad_no)
        if player is None:
            squad_row = squad_by_no.get(squad_no)
            if squad_row is None:
                continue
            player = _xi_row_from_squad(squad_row)

        position = str(player["position"]).upper()
        same_pos = [row for row in xi if str(row["position"]).upper() == position]
        if same_pos:
            drop = min(
                same_pos,
                key=lambda row: (
                    row.get("minutes_share") is None,
                    float(row.get("minutes_share") or 0.0),
                ),
            )
        elif xi:
            drop = min(
                xi,
                key=lambda row: (
                    row.get("minutes_share") is None,
                    float(row.get("minutes_share") or 0.0),
                ),
            )
        else:
            xi.append(player)
            xi_nos.add(squad_no)
            continue

        xi = [row for row in xi if int(row["squad_no"]) != int(drop["squad_no"])]
        xi.append(player)
        xi_nos.add(squad_no)

    selected_nos = {int(row["squad_no"]) for row in xi}
    new_bench = [row for row in matched if int(row["squad_no"]) not in selected_nos]
    new_bench.sort(
        key=lambda row: (
            row.get("minutes_share") is None,
            -(float(row["minutes_share"]) if row.get("minutes_share") is not None else 0.0),
            str(row["player_name"]).lower(),
        )
    )
    return xi, new_bench


def resolve_team_code_for_id(team_id: int) -> str | None:
    """Resolve FIFA team_code for a SportMonks team_id (Neon teams, then manual)."""
    try:
        from src.database import get_fifa_code_for_team_id
    except ImportError:
        return _manual_team_code_for_id(team_id)
    return get_fifa_code_for_team_id(team_id) or _manual_team_code_for_id(team_id)


def empty_team_player_display(
    *,
    status: str = "no_qualifier_data",
) -> dict[str, Any]:
    """Default per-team player_ratings block when display data is unavailable."""
    return {
        "projected_xi": [],
        "bench": [],
        "status": status,
        "squad": [],
        "ratings_source": "none",
    }


def _ratings_by_squad_no(team_ratings: dict[str, Any]) -> dict[int, dict[str, Any]]:
    by_squad: dict[int, dict[str, Any]] = {}
    for bucket in ("listed", "insufficient_data"):
        for row in team_ratings.get(bucket, []):
            squad_no = row.get("squad_no")
            if squad_no is None:
                continue
            by_squad[int(squad_no)] = row
    return by_squad


def _serialize_squad_rows(
    squad_rows: list[dict[str, Any]],
    *,
    ratings_by_squad: dict[int, dict[str, Any]] | None = None,
    attach_ratings: bool = False,
) -> list[dict[str, Any]]:
    """Build the 26-row FIFA squad list for report JSON."""
    serialized: list[dict[str, Any]] = []
    for row in squad_rows:
        entry: dict[str, Any] = {}
        for field in _SQUAD_DISPLAY_FIELDS:
            value = row.get(field)
            if field == "squad_no" and value is not None:
                entry[field] = int(value)
            elif field == "height_cm" and value is not None:
                entry[field] = int(value)
            elif field == "dob" and value is not None:
                entry[field] = str(value)
            elif value is not None and value != "":
                entry[field] = value
        if attach_ratings and ratings_by_squad:
            rating = ratings_by_squad.get(int(row["squad_no"]))
            if rating is not None:
                entry["avg_rating"] = rating["avg_rating"]
                entry["matches_counted"] = int(rating["matches_counted"])
        serialized.append(entry)
    return serialized


def build_team_player_display_block(
    team_ratings: dict[str, Any],
    squad_rows: list[dict[str, Any]],
    id_map: list[dict[str, Any]],
    formation: tuple[int, int, int, int] = _DEFAULT_FORMATION,
    *,
    team_code: str | None = None,
    team_id: int | None = None,
    client: SportmonksClient | None = None,
) -> dict[str, Any]:
    """Assemble projected XI, bench, status, and full squad for one team."""
    squad_to_sm = _lineup_squad_to_sm_id(team_code, id_map)
    xi = build_projected_xi(
        team_ratings,
        squad_rows,
        id_map,
        formation=formation,
        team_code=team_code,
    )
    if client is not None:
        xi = {
            **xi,
            "projected_xi": _enrich_missing_lineup_sm_ids(
                xi["projected_xi"],
                squad_rows,
                squad_to_sm,
                client=client,
                team_id=team_id,
            ),
            "bench": _enrich_missing_lineup_sm_ids(
                xi["bench"],
                squad_rows,
                squad_to_sm,
                client=client,
                team_id=team_id,
            ),
        }
    attach = xi["status"] == "manual_ratings_no_xi"
    ratings_by_squad = _ratings_by_squad_no(team_ratings) if attach else None
    squad = _serialize_squad_rows(
        squad_rows,
        ratings_by_squad=ratings_by_squad,
        attach_ratings=attach,
    )
    return {
        "projected_xi": xi["projected_xi"],
        "bench": xi["bench"],
        "status": xi["status"],
        "squad": squad,
        "ratings_source": str(team_ratings.get("source") or "none"),
    }


def build_team_player_display_for_code(
    team_code: str,
    team_id: int,
    client: SportmonksClient,
) -> dict[str, Any]:
    """Load Neon reference data and build the per-team display block. Fail-soft."""
    try:
        from src.database import (
            DatabaseError,
            get_player_id_map_for_team,
            get_wc2026_squad_for_team,
        )
    except ImportError:
        return empty_team_player_display()

    try:
        squad_rows = get_wc2026_squad_for_team(team_code)
        id_map = get_player_id_map_for_team(team_code)
    except DatabaseError as exc:
        logger.warning(
            "Could not load Neon squad/id_map for %s: %s", team_code, exc
        )
        return empty_team_player_display()

    if not squad_rows:
        return empty_team_player_display()

    team_ratings = build_player_ratings_for_team(client, team_id)
    return build_team_player_display_block(
        team_ratings,
        squad_rows,
        id_map,
        team_code=team_code,
        team_id=team_id,
        client=client,
    )


def build_matchup_player_display(
    client: SportmonksClient,
    team_a_id: int,
    team_b_id: int,
    *,
    cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the ``player_ratings`` section; cache blocks by team_code."""
    store = cache if cache is not None else {}

    def _block(team_id: int) -> dict[str, Any]:
        team_code = resolve_team_code_for_id(team_id)
        if not team_code:
            return empty_team_player_display()
        if team_code in store:
            return store[team_code]
        built = build_team_player_display_for_code(team_code, team_id, client)
        store[team_code] = built
        return built

    return {
        "team_a": _block(team_a_id),
        "team_b": _block(team_b_id),
    }


def build_matchup_player_ratings(
    client: SportmonksClient,
    team_a_id: int,
    team_b_id: int,
) -> dict[str, Any]:
    """Build the ``player_ratings`` section for a two-team matchup report."""
    return {
        "team_a": build_player_ratings_for_team(client, team_a_id),
        "team_b": build_player_ratings_for_team(client, team_b_id),
    }
