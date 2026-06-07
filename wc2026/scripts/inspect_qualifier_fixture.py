"""Diagnostic: what stats are available on a single qualifier fixture?

Before we commit to the model design we need to confirm whether SportMonks
returns xG, big chances, shots on target, and xGOT for international WC
qualifier matches. This script fetches one qualifier fixture (WC Qualification
Europe), dumps the raw response, and cross-references its statistics against
the /types catalog to see which stat types are actually present.

Purely diagnostic -- NO classifier or parser logic.

Run from the project root (cached after first run):

    python scripts/inspect_qualifier_fixture.py
"""

from __future__ import annotations

import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

# Allow direct execution by putting the project root on sys.path.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402

logger = logging.getLogger("inspect_qualifier_fixture")

CATALOG_PATH = _PROJECT_ROOT / "data" / "leagues_catalog.json"
STAT_TYPES_PATH = _PROJECT_ROOT / "data" / "stat_types.json"
SAMPLE_FIXTURE_PATH = _PROJECT_ROOT / "data" / "sample_fixture.json"

# The /types catalog lives under the SportMonks "core" base, not "football".
CORE_BASE_URL = "https://api.sportmonks.com/v3/core"

EUROPE_LEAGUE_NAME = "WC Qualification Europe"
EUROPE_LEAGUE_ID_FALLBACK = 720

FIXTURE_INCLUDES = "participants;scores;statistics;state"

# Stat detection: label -> substrings to look for in a stat type's name.
REQUIRED_STATS: dict[str, list[str]] = {
    "Expected goals (xG)": ["expected goals", "xg"],
    "Big chances created": ["big chance"],
    "Shots on target": ["shots on target", "shots on goal"],
    "xGOT (expected goals on target)": ["expected goals on target", "xgot"],
    "Goals": ["goals"],
    "Possession": ["possession"],
    "Total shots": ["shots total", "total shots"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_europe_league_id() -> int:
    """Find the WC Qualification Europe league id from the saved catalog."""
    if CATALOG_PATH.exists():
        try:
            catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
            for league in catalog:
                if league.get("name") == EUROPE_LEAGUE_NAME:
                    return int(league["id"])
        except (ValueError, KeyError, TypeError):
            logger.warning("Could not parse catalog; using fallback id.")
    logger.info("Using fallback Europe league id %s", EUROPE_LEAGUE_ID_FALLBACK)
    return EUROPE_LEAGUE_ID_FALLBACK


def log_rate_limit(response: dict[str, Any], when: str) -> None:
    rl = response.get("rate_limit")
    if isinstance(rl, dict):
        logger.info(
            "Rate limit %s: %s remaining (resets in %ss)",
            when,
            rl.get("remaining"),
            rl.get("resets_in_seconds"),
        )


def fetch_fixtures(client: SportmonksClient, league_id: int) -> dict[str, Any]:
    """Fetch a few fixtures for the league, trying a couple of filter styles.

    SportMonks v3 fixture filtering is finicky, so we try the direct league
    filter first, then fall back to filtering by the league's most recent
    season. Returns the first response that yields a non-empty ``data`` list,
    or the last response tried.
    """
    attempts: list[tuple[str, str, dict[str, Any]]] = [
        (
            "fixtures filtered by league",
            "fixtures",
            {
                "filters": f"fixtureLeagues:{league_id}",
                "include": FIXTURE_INCLUDES,
                "per_page": 5,
            },
        ),
    ]

    # Add a season-based fallback if we can discover a season id.
    season_id = _discover_latest_season(client, league_id)
    if season_id is not None:
        attempts.append(
            (
                f"fixtures filtered by season {season_id}",
                "fixtures",
                {
                    "filters": f"fixtureSeasons:{season_id}",
                    "include": FIXTURE_INCLUDES,
                    "per_page": 5,
                },
            )
        )

    last_response: dict[str, Any] = {}
    for label, endpoint, params in attempts:
        logger.info("Attempt: %s", label)
        try:
            response = client.get(endpoint, params=params)
        except SportmonksError as exc:
            logger.warning("Attempt '%s' failed: %s", label, exc)
            continue
        last_response = response
        data = response.get("data")
        if isinstance(data, list) and data:
            logger.info("Attempt '%s' returned %d fixtures.", label, len(data))
            return response
        logger.info("Attempt '%s' returned no fixtures.", label)

    return last_response


def _discover_latest_season(
    client: SportmonksClient, league_id: int
) -> int | None:
    """Try to find the league's most recent season id (best-effort)."""
    try:
        response = client.get(
            f"leagues/{league_id}", params={"include": "seasons"}
        )
    except SportmonksError as exc:
        logger.warning("Could not fetch seasons for league %s: %s", league_id, exc)
        return None

    league = response.get("data") or {}
    seasons = league.get("seasons")
    if not isinstance(seasons, list) or not seasons:
        return None

    # Prefer a season flagged current; otherwise the highest id.
    for season in seasons:
        if season.get("is_current"):
            return int(season["id"])
    return max(int(s["id"]) for s in seasons if "id" in s)


def fetch_stat_types() -> dict[int, str]:
    """Fetch the /types catalog and build a {type_id: name} map.

    Saves the raw types to data/stat_types.json. Paginates a few pages so the
    map is reasonably complete without burning quota. Uses the SportMonks
    "core" base URL, where the /types catalog actually lives.
    """
    core_client = SportmonksClient(base_url=CORE_BASE_URL)
    all_types: list[dict[str, Any]] = []
    page = 1
    while page <= 20:  # safety cap
        try:
            response = core_client.get(
                "types", params={"per_page": 100, "page": page}
            )
        except SportmonksError as exc:
            logger.warning("Could not fetch /types page %s: %s", page, exc)
            break
        data = response.get("data")
        if not isinstance(data, list) or not data:
            break
        all_types.extend(data)
        pagination = response.get("pagination") or {}
        if not pagination.get("has_more"):
            break
        page += 1

    STAT_TYPES_PATH.parent.mkdir(parents=True, exist_ok=True)
    STAT_TYPES_PATH.write_text(
        json.dumps(all_types, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    logger.info("Saved %d types to %s", len(all_types), STAT_TYPES_PATH)

    type_map: dict[int, str] = {}
    for t in all_types:
        if "id" in t and "name" in t:
            type_map[int(t["id"])] = str(t["name"])
    return type_map


def pick_fixture_with_stats(fixtures: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the first fixture that has non-empty statistics, else the first."""
    for fixture in fixtures:
        stats = fixture.get("statistics")
        if isinstance(stats, list) and stats:
            return fixture
    return fixtures[0]


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def analyze_fixture(fixture: dict[str, Any], type_map: dict[int, str]) -> dict[str, bool | None]:
    """Print a structured analysis of the fixture; return stat-presence flags."""
    print("\n" + "=" * 70)
    print("STRUCTURED ANALYSIS")
    print("=" * 70)
    print(f"Top-level fixture keys: {sorted(fixture.keys())}")

    participants = fixture.get("participants")
    if isinstance(participants, list) and participants:
        print(f"\nparticipants: {len(participants)} team(s)")
        print(f"  fields per team: {sorted(participants[0].keys())}")
        for p in participants:
            meta = p.get("meta", {})
            print(
                f"  - id={p.get('id')} name={p.get('name')!r} "
                f"location={meta.get('location') if isinstance(meta, dict) else '?'}"
            )
    else:
        print("\nparticipants: (none returned)")

    scores = fixture.get("scores")
    if isinstance(scores, list) and scores:
        print(f"\nscores: {len(scores)} entries; fields: {sorted(scores[0].keys())}")
        for s in scores[:8]:
            print(f"  - {json.dumps(s, ensure_ascii=False)}")
    else:
        print("\nscores: (none returned)")

    statistics = fixture.get("statistics")
    present_type_ids: set[int] = set()
    print("\nstatistics:")
    if isinstance(statistics, list) and statistics:
        print(f"  {len(statistics)} stat entries; fields: {sorted(statistics[0].keys())}")
        by_team: dict[Any, list[dict[str, Any]]] = defaultdict(list)
        for stat in statistics:
            team_id = stat.get("participant_id") or stat.get("team_id")
            by_team[team_id].append(stat)
            tid = stat.get("type_id")
            if isinstance(tid, int):
                present_type_ids.add(tid)
        for team_id, stats in by_team.items():
            print(f"\n  team_id={team_id} -> {len(stats)} stats:")
            for stat in stats:
                tid = stat.get("type_id")
                name = type_map.get(tid, "<unknown type>") if isinstance(tid, int) else "?"
                value = _extract_value(stat)
                print(f"    type_id={tid} ({name}) = {value}")
    else:
        print("  (none returned)")

    return _check_required_stats(present_type_ids, type_map)


def _extract_value(stat: dict[str, Any]) -> Any:
    """Pull the numeric value out of a stat entry, handling shape variants."""
    data = stat.get("data")
    if isinstance(data, dict) and "value" in data:
        return data["value"]
    if "value" in stat:
        return stat["value"]
    return data if data is not None else "<no value>"


def _check_required_stats(
    present_type_ids: set[int], type_map: dict[int, str]
) -> dict[str, bool | None]:
    """Report PRESENT/MISSING for each required model stat."""
    print("\n" + "=" * 70)
    print("REQUIRED-STAT CHECK")
    print("=" * 70)

    # Build name -> id index from the types map for substring matching.
    present_names = {type_map[tid].lower() for tid in present_type_ids if tid in type_map}

    results: dict[str, bool | None] = {}
    for label, needles in REQUIRED_STATS.items():
        # A stat counts as present if any present type name matches a needle.
        matched = any(
            any(needle in name for needle in needles) for name in present_names
        )
        # If the type catalog is empty we cannot judge by name -> unclear.
        if not type_map:
            results[label] = None
            status = "UNCLEAR (no type catalog)"
        else:
            results[label] = matched
            status = "PRESENT" if matched else "MISSING"
        print(f"  {label:<35} {status}")
    return results


def print_final_summary(
    results: dict[str, bool | None], num_stat_types: int
) -> None:
    print("\n" + "=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)

    def verdict(label: str) -> str:
        val = results.get(label)
        if val is None:
            return "UNCLEAR"
        return "YES" if val else "NO"

    xg = verdict("Expected goals (xG)")
    big = verdict("Big chances created")
    xgot = verdict("xGOT (expected goals on target)")

    print(f"  Plan supports xG:          {xg}")
    print(f"  Plan supports big chances: {big}")
    print(f"  Plan supports xGOT:        {xgot}")
    print(f"  Stat types on this fixture: {num_stat_types}")

    core_ok = xg == "YES" and big == "YES" and xgot == "YES"
    if core_ok:
        rec = "model design is viable"
    elif "YES" in (xg, big, xgot):
        rec = "model needs revision (some advanced stats missing)"
    else:
        rec = "model needs revision (advanced stats unavailable)"
    print(f"  Recommendation: {rec}")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    print("=" * 70)
    print("Qualifier fixture stat inspection")
    print("=" * 70)

    try:
        client = SportmonksClient()
    except SportmonksError as exc:
        print(f"\n[CONFIG ERROR] {exc}")
        return 1

    try:
        league_id = load_europe_league_id()
        print(f"Inspecting league id {league_id} ({EUROPE_LEAGUE_NAME})")

        fixtures_response = fetch_fixtures(client, league_id)
        log_rate_limit(fixtures_response, "at start")

        fixtures = fixtures_response.get("data")
        if not isinstance(fixtures, list) or not fixtures:
            print("\n[WARNING] No fixtures returned for this league.")
            print("Full response for diagnosis:")
            print(json.dumps(fixtures_response, indent=2, ensure_ascii=False)[:4000])
            return 1

        type_map = fetch_stat_types()

        fixture = pick_fixture_with_stats(fixtures)

        SAMPLE_FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
        SAMPLE_FIXTURE_PATH.write_text(
            json.dumps(fixture, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        print("\n" + "=" * 70)
        print("FULL RAW FIXTURE RESPONSE (first fixture with stats)")
        print("=" * 70)
        print(json.dumps(fixture, indent=2, ensure_ascii=False))

        results = analyze_fixture(fixture, type_map)

        stats = fixture.get("statistics")
        num_types = len({s.get("type_id") for s in stats}) if isinstance(stats, list) else 0
        print_final_summary(results, num_types)

        print(f"\nSaved sample fixture to {SAMPLE_FIXTURE_PATH}")
        print(f"Saved stat types to {STAT_TYPES_PATH}")
    except SportmonksError as exc:
        print(f"\n[REQUEST FAILED] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - diagnostic; surface anything
        print(f"\n[UNEXPECTED ERROR] {type(exc).__name__}: {exc}")
        return 1

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
