"""Targeted diagnostic: is xG data available on our plan for recent qualifiers?

The previous diagnostic sampled a 2017 fixture (no xG). xG coverage is usually
only populated for recent matches and may require a specific include or add-on.
This script finds the most recent FINISHED WC Qualification Europe fixture and
probes it with several SportMonks v3 include strategies to definitively answer:
can we obtain xG / expected-goals data?

Purely diagnostic -- NO classifier or parser logic.

Run from the project root (cached where possible):

    python scripts/inspect_recent_qualifier_xg.py
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

# Allow direct execution by putting the project root on sys.path.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402

logger = logging.getLogger("inspect_recent_qualifier_xg")

CATALOG_PATH = _PROJECT_ROOT / "data" / "leagues_catalog.json"
RESPONSES_PATH = _PROJECT_ROOT / "data" / "xg_diagnostic_responses.json"

CORE_BASE_URL = "https://api.sportmonks.com/v3/core"

EUROPE_LEAGUE_NAME = "WC Qualification Europe"
EUROPE_LEAGUE_ID_FALLBACK = 720

# A fixture is treated as finished if its state_id is one of these (FT / AET /
# FT after penalties). Falls back to a result_info + past-date heuristic.
FINISHED_STATE_IDS = {5, 7, 8}

XG_KEYWORDS = ("expected", "xg")  # 'xG'.lower() == 'xg'

# Everything we collect, dumped at the end for reference.
collected: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def load_europe_league_id() -> int:
    if CATALOG_PATH.exists():
        try:
            for league in json.loads(CATALOG_PATH.read_text(encoding="utf-8")):
                if league.get("name") == EUROPE_LEAGUE_NAME:
                    return int(league["id"])
        except (ValueError, KeyError, TypeError):
            logger.warning("Could not parse catalog; using fallback id.")
    return EUROPE_LEAGUE_ID_FALLBACK


def log_rate_limit(response: dict[str, Any], when: str) -> None:
    rl = response.get("rate_limit") if isinstance(response, dict) else None
    if isinstance(rl, dict):
        logger.info(
            "Rate limit %s: %s remaining (resets in %ss)",
            when, rl.get("remaining"), rl.get("resets_in_seconds"),
        )


def find_xg_fields(obj: Any, path: str = "") -> list[dict[str, Any]]:
    """Recursively find keys or string values mentioning expected/xg."""
    hits: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_path = f"{path}.{key}" if path else str(key)
            if any(kw in str(key).lower() for kw in XG_KEYWORDS):
                preview = value if not isinstance(value, (dict, list)) else f"<{type(value).__name__}>"
                hits.append({"path": key_path, "match": "key", "value": preview})
            hits.extend(find_xg_fields(value, key_path))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            hits.extend(find_xg_fields(item, f"{path}[{i}]"))
    elif isinstance(obj, str):
        if any(kw in obj.lower() for kw in XG_KEYWORDS):
            hits.append({"path": path, "match": "value", "value": obj})
    return hits


# ---------------------------------------------------------------------------
# Phase 1: find a recent finished qualifier fixture
# ---------------------------------------------------------------------------
def _discover_current_season(client: SportmonksClient, league_id: int) -> int | None:
    try:
        response = client.get(f"leagues/{league_id}", params={"include": "seasons"})
    except SportmonksError as exc:
        logger.warning("Could not fetch seasons: %s", exc)
        return None
    seasons = (response.get("data") or {}).get("seasons")
    if not isinstance(seasons, list) or not seasons:
        return None
    for season in seasons:
        if season.get("is_current"):
            return int(season["id"])
    return max(int(s["id"]) for s in seasons if "id" in s)


def find_recent_finished_fixture(
    client: SportmonksClient, league_id: int
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Return (fixture, raw_response) for the most recent finished fixture.

    Targets the league's current season (the league filter alone surfaces very
    old fixtures), then sorts client-side by kickoff time descending.
    """
    season_id = _discover_current_season(client, league_id)
    logger.info("Current season for league %s: %s", league_id, season_id)

    attempts: list[dict[str, Any]] = []
    if season_id is not None:
        attempts.append({"filters": f"fixtureSeasons:{season_id}", "per_page": 25})
    attempts.append({"filters": f"fixtureLeagues:{league_id}", "per_page": 25})

    last_response: dict[str, Any] = {}
    for params in attempts:
        try:
            response = client.get("fixtures", params=params)
        except SportmonksError as exc:
            logger.warning("Fixture fetch failed for %s: %s", params, exc)
            continue
        last_response = response
        fixtures = response.get("data")
        if not isinstance(fixtures, list) or not fixtures:
            continue

        now = time.time()
        finished = [
            f for f in fixtures
            if f.get("state_id") in FINISHED_STATE_IDS
            or (f.get("result_info") and (f.get("starting_at_timestamp") or 0) <= now)
        ]
        pool = finished or fixtures
        pool.sort(key=lambda f: f.get("starting_at_timestamp") or 0, reverse=True)
        logger.info(
            "Got %d fixtures (%d finished) via %s",
            len(fixtures), len(finished), params["filters"],
        )
        return pool[0], response

    return None, last_response


# ---------------------------------------------------------------------------
# Phase 2: probe xG with multiple include strategies
# ---------------------------------------------------------------------------
def probe_xg_strategies(
    client: SportmonksClient, fixture_id: int
) -> dict[str, Any]:
    """Try each include strategy; record status + any xG-looking fields."""
    strategies: list[tuple[str, str, dict[str, Any] | None]] = [
        ("A: include=xGFixture", f"fixtures/{fixture_id}", {"include": "xGFixture"}),
        ("B: include=expectedFixtures", f"fixtures/{fixture_id}", {"include": "expectedFixtures"}),
        ("C: include=statistics.type;participants;scores", f"fixtures/{fixture_id}",
         {"include": "statistics.type;participants;scores"}),
        ("D: kitchen sink", f"fixtures/{fixture_id}",
         {"include": "events;statistics;participants;scores;xGFixture;expectedFixtures"}),
        ("E: /expected/fixtures/{id}", f"expected/fixtures/{fixture_id}", None),
    ]

    results: dict[str, Any] = {}
    for label, endpoint, params in strategies:
        print("\n" + "-" * 70)
        print(f"STRATEGY {label}")
        print("-" * 70)
        entry: dict[str, Any] = {"endpoint": endpoint, "params": params}
        try:
            response = client.get(endpoint, params=params)
            entry["status"] = "200 OK"
            entry["response"] = response
            xg_hits = find_xg_fields(response.get("data", response))
            entry["xg_hits"] = xg_hits
            print("Status: 200 OK")
            if xg_hits:
                print(f"xG-related fields found ({len(xg_hits)}):")
                for hit in xg_hits[:40]:
                    print(f"  [{hit['match']}] {hit['path']} = {hit['value']}")
            else:
                print("xG-related fields found: NONE")
        except SportmonksError as exc:
            entry["status"] = "ERROR"
            entry["error"] = str(exc)
            entry["xg_hits"] = []
            print(f"Status: FAILED -> {exc}")
        results[label] = entry
    return results


# ---------------------------------------------------------------------------
# Phase 3: subscription / add-on details
# ---------------------------------------------------------------------------
def probe_subscription(
    client: SportmonksClient, any_response: dict[str, Any]
) -> dict[str, Any]:
    print("\n" + "=" * 70)
    print("PHASE 3: SUBSCRIPTION / ADD-ONS")
    print("=" * 70)

    out: dict[str, Any] = {}

    subscription = any_response.get("subscription") if isinstance(any_response, dict) else None
    print("\nSubscription block from a normal response:")
    print(json.dumps(subscription, indent=2, ensure_ascii=False))
    out["subscription_block"] = subscription

    core_client = SportmonksClient(base_url=CORE_BASE_URL)
    for endpoint in ("my/resources", "my/usage", "my/enrichments", "me"):
        try:
            resp = core_client.get(endpoint)
            out[endpoint] = {"status": "200 OK", "response": resp}
            print(f"\n[{endpoint}] 200 OK")
            print(json.dumps(resp, indent=2, ensure_ascii=False)[:2000])
        except SportmonksError as exc:
            out[endpoint] = {"status": "ERROR", "error": str(exc)}
            print(f"\n[{endpoint}] not available -> {exc}")
    return out


# ---------------------------------------------------------------------------
# Phase 4: summary
# ---------------------------------------------------------------------------
def print_summary(fixture: dict[str, Any], strategy_results: dict[str, Any]) -> None:
    working = None
    sample_path = None
    sample_value = None
    for label, entry in strategy_results.items():
        if entry.get("status") == "200 OK" and entry.get("xg_hits"):
            working = label
            hit = entry["xg_hits"][0]
            sample_path = hit["path"]
            sample_value = hit["value"]
            break

    name = fixture.get("name")
    starting = fixture.get("starting_at")
    fid = fixture.get("id")

    print("\n" + "=" * 70)
    print("CURRENT FINDINGS")
    print("=" * 70)
    print(f"- Recent qualifier fixture inspected: id={fid} | {name} | {starting}")
    print(f"- xG include strategy that worked: {working or 'NONE'}")
    print(f"- xG data present on this fixture: {'YES' if working else 'NO'}")
    print(f"- xG field path in response (if present): {sample_path or 'N/A'}")
    print(f"- Sample xG value (if present): {sample_value if sample_value is not None else 'N/A'}")

    print("\n" + "=" * 70)
    print("NEXT STEPS")
    print("=" * 70)
    if working:
        print("xG IS available. Proceed with the current model design and update")
        print(f"src/sportmonks_client.py to use the working include: {working}.")
    else:
        print("xG NOT found. Recommended: contact SportMonks support and ask:")
        print("  1. Does the 'World Cup 2026 All-in' plan include xG data for")
        print("     international qualifier fixtures?")
        print("  2. If yes, what is the correct include parameter or endpoint?")
        print("  3. If no, what add-on or plan upgrade enables xG for these fixtures?")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    print("=" * 70)
    print("Recent qualifier xG availability diagnostic")
    print("=" * 70)

    try:
        client = SportmonksClient()
    except SportmonksError as exc:
        print(f"\n[CONFIG ERROR] {exc}")
        return 1

    try:
        league_id = load_europe_league_id()

        print("\n" + "=" * 70)
        print("PHASE 1: FIND A RECENT FINISHED QUALIFIER FIXTURE")
        print("=" * 70)
        fixture, finder_response = find_recent_finished_fixture(client, league_id)
        log_rate_limit(finder_response, "at start")
        collected["phase1_finder_response"] = finder_response

        if not fixture:
            print("\n[WARNING] Could not find any fixture for this league.")
            collected["phase1_fixture"] = None
            RESPONSES_PATH.write_text(
                json.dumps(collected, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            return 1

        collected["phase1_fixture"] = fixture
        print(f"\nMost recent finished fixture:")
        print(f"  id           : {fixture.get('id')}")
        print(f"  name         : {fixture.get('name')}")
        print(f"  starting_at  : {fixture.get('starting_at')}")
        print(f"  state_id     : {fixture.get('state_id')}")
        print(f"  result_info  : {fixture.get('result_info')}")

        print("\n" + "=" * 70)
        print("PHASE 2: PROBE xG VIA MULTIPLE INCLUDE STRATEGIES")
        print("=" * 70)
        strategy_results = probe_xg_strategies(client, int(fixture["id"]))
        collected["phase2_strategies"] = strategy_results

        # Pick any successful response to read the subscription block from.
        any_ok = next(
            (e["response"] for e in strategy_results.values()
             if e.get("status") == "200 OK" and isinstance(e.get("response"), dict)),
            finder_response,
        )
        collected["phase3_subscription"] = probe_subscription(client, any_ok)
        log_rate_limit(any_ok, "at end")

        print_summary(fixture, strategy_results)
    except SportmonksError as exc:
        print(f"\n[REQUEST FAILED] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - diagnostic; surface anything
        print(f"\n[UNEXPECTED ERROR] {type(exc).__name__}: {exc}")
        return 1
    finally:
        RESPONSES_PATH.parent.mkdir(parents=True, exist_ok=True)
        RESPONSES_PATH.write_text(
            json.dumps(collected, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        print(f"\nSaved all diagnostic responses to {RESPONSES_PATH}")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
