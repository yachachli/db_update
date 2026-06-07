"""Live end-to-end test of the fetch + parse layer against SportMonks.

Confirms that get_fixtures_for_team() + get_fixture_by_id() +
parse_fixture_to_match_stats() work against real data before we build the
orchestration layer on top. Uses Estonia (known to have recent qualifier
matches). Cached on re-run; exits non-zero on any failure or zero results.

Run from the project root:

    python scripts/test_fetcher_live.py
"""

from __future__ import annotations

import dataclasses
import json
import logging
import sys
from pathlib import Path
from typing import Any

# Allow direct execution by putting the project root on sys.path.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402
from src.sportmonks_parser import parse_fixture_to_match_stats  # noqa: E402

logger = logging.getLogger("test_fetcher_live")

SEARCH_TEAM = "Estonia"
FIXTURE_LIMIT = 10
PLACEHOLDER_OPPONENT_FIFA = 1000.0  # hardcoded for this smoke test


def log_rate_limit(response: dict[str, Any], when: str) -> None:
    rl = response.get("rate_limit") if isinstance(response, dict) else None
    if isinstance(rl, dict):
        logger.info(
            "Rate limit %s: %s remaining (resets in %ss)",
            when, rl.get("remaining"), rl.get("resets_in_seconds"),
        )


# ---------------------------------------------------------------------------
# Phase 1
# ---------------------------------------------------------------------------
def resolve_team_id(client: SportmonksClient) -> tuple[int, str, dict[str, Any]]:
    """Search for the senior men's national team and return (id, name, resp)."""
    response = client.get(f"teams/search/{SEARCH_TEAM}")
    candidates = response.get("data")
    if not isinstance(candidates, list) or not candidates:
        raise SportmonksError(f"No teams found for search '{SEARCH_TEAM}'.")

    # Prefer an exact-name national team; fall back to any national team.
    def is_national(team: dict[str, Any]) -> bool:
        return team.get("type") == "national"

    exact = [
        t for t in candidates
        if is_national(t) and str(t.get("name", "")).lower() == SEARCH_TEAM.lower()
    ]
    nationals = [t for t in candidates if is_national(t)]
    chosen = (exact or nationals or candidates)[0]
    return int(chosen["id"]), str(chosen.get("name", "")), response


# ---------------------------------------------------------------------------
# Phase 2
# ---------------------------------------------------------------------------
def _participant_names(fixture: dict[str, Any]) -> str:
    names = [p.get("name", "?") for p in fixture.get("participants", [])]
    return " vs ".join(names) if names else fixture.get("name", "?")


def summarize_fixture(fixture: dict[str, Any]) -> str:
    return (
        f"id={fixture.get('id')} | {fixture.get('starting_at')} | "
        f"{_participant_names(fixture)} | {fixture.get('result_info')}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    print("=" * 70)
    print("Live fetch + parse smoke test")
    print("=" * 70)

    try:
        client = SportmonksClient()
    except SportmonksError as exc:
        print(f"\n[CONFIG ERROR] {exc}")
        return 1

    try:
        # ---- Phase 1 ----
        print("\n" + "=" * 70)
        print("PHASE 1: RESOLVE TEAM ID")
        print("=" * 70)
        team_id, team_name, search_resp = resolve_team_id(client)
        log_rate_limit(search_resp, "at start")
        print(f"Resolved team_id={team_id}, name={team_name!r}")

        # ---- Phase 2 ----
        print("\n" + "=" * 70)
        print("PHASE 2: FETCH FIXTURES")
        print("=" * 70)
        fixtures = client.get_fixtures_for_team(team_id, limit=FIXTURE_LIMIT)
        print(f"Fixtures returned: {len(fixtures)}")

        if not fixtures:
            print(
                "\n[WARNING] Zero fixtures returned. The 'fixtureParticipants' "
                "filter token may be wrong, or this team has no fixtures in our "
                "league universe. Investigate before building orchestration."
            )
            return 1

        for fixture in fixtures:
            print(f"  {summarize_fixture(fixture)}")

        # ---- Phase 3 ----
        print("\n" + "=" * 70)
        print("PHASE 3: PARSE EACH FIXTURE")
        print("=" * 70)
        parsed: list[Any] = []
        failures: list[tuple[Any, str]] = []
        for fixture in fixtures:
            fid = fixture.get("id")
            try:
                stats = parse_fixture_to_match_stats(
                    fixture, team_id, PLACEHOLDER_OPPONENT_FIFA
                )
            except Exception as exc:  # noqa: BLE001 - report which fixture failed
                failures.append((fid, f"{type(exc).__name__}: {exc}"))
                print(f"  [PARSE FAILED] fixture {fid}: {exc}")
                continue
            parsed.append((fixture, stats))
            print(
                f"  {stats.date:%Y-%m-%d}  {stats.outcome} {stats.scoreline_str}  "
                f"xG: {stats.xg_created:.1f} vs {stats.xg_conceded:.1f}  "
                f"Poss: {stats.possession_pct:.0f}%"
            )

        # ---- Phase 4 ----
        print("\n" + "=" * 70)
        print("PHASE 4: DEEP SPOT-CHECK (first parsed fixture)")
        print("=" * 70)
        if parsed:
            fixture, stats = parsed[0]
            print("\nParsed MatchStats fields:")
            print(json.dumps(dataclasses.asdict(stats), indent=2, default=str))
            print("\nRaw SportMonks fixture for cross-reference:")
            print(json.dumps(fixture, indent=2, ensure_ascii=False))
        else:
            print("No fixtures parsed successfully; nothing to spot-check.")

        # ---- Phase 5 ----
        print("\n" + "=" * 70)
        print("PHASE 5: FINAL SUMMARY")
        print("=" * 70)
        print(f"Total fixtures fetched   : {len(fixtures)}")
        print(f"Total successfully parsed: {len(parsed)}")
        print(f"Parse failures           : {len(failures)}")
        for fid, reason in failures:
            print(f"  - fixture {fid} (team_id={team_id}): {reason}")

        # Fresh call to read current rate limit (bypass cache).
        final_resp = client.get(
            f"teams/search/{SEARCH_TEAM}", force_refresh=True
        )
        log_rate_limit(final_resp, "at end")

        if failures:
            print("\n[RESULT] Completed with parse failures.")
            return 1
    except SportmonksError as exc:
        print(f"\n[REQUEST FAILED] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - surface any regression
        print(f"\n[UNEXPECTED ERROR] {type(exc).__name__}: {exc}")
        return 1

    print("\n[RESULT] All fixtures fetched and parsed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
