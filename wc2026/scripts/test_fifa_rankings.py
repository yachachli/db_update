"""End-to-end test of src/fifa_rankings.py against the live FIFA API.

Exercises dateId discovery (walk-back + cache), release fetching/parsing, and
team lookups, cross-checking the result against the known 1 Apr 2026 release
(France #1 @ 1877.32, Spain #2 @ 1876.40).

Run from the project root:

    python scripts/test_fifa_rankings.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Allow standalone execution (`python scripts/test_fifa_rankings.py`).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.fifa_rankings import FifaRankingsClient, FifaRankingsError  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


def main() -> int:
    print("=" * 70)
    print("FIFA RANKINGS CLIENT — END-TO-END TEST")
    print("=" * 70)

    client = FifaRankingsClient()

    # -- 2. Force a fresh walk so we see it actually run -------------------
    print("\n[2] discover_latest_date_id(force_refresh=True) — expect a walk")
    calls_before = client.api_call_count
    date_id = client.discover_latest_date_id(force_refresh=True)
    walk_calls = client.api_call_count - calls_before
    print(f"    resolved dateId: {date_id}")
    print(f"    API calls during walk: {walk_calls}")

    # -- 3. Show resolved release date -------------------------------------
    print("\n[3] Resolved release")
    release = client.fetch_ranking(date_id)
    print(f"    dateId={release.date_id}  release_date={release.release_date}  "
          f"teams={len(release)}")
    assert date_id == "id15065", f"expected id15065, got {date_id}"
    assert release.release_date == "2026-04-01", \
        f"expected 2026-04-01, got {release.release_date}"

    # -- 4. Second discover WITHOUT force — must hit cache (0 API calls) ----
    print("\n[4] discover_latest_date_id() again — expect instant cache hit")
    calls_before = client.api_call_count
    cached_date_id = client.discover_latest_date_id()
    cache_calls = client.api_call_count - calls_before
    print(f"    resolved dateId: {cached_date_id}")
    print(f"    API calls this time: {cache_calls}")
    assert cache_calls == 0, f"expected 0 API calls on cache hit, got {cache_calls}"
    assert cached_date_id == date_id, "cache returned a different dateId"
    print("    PROOF: cache hit served with zero API calls.")

    # -- 5. Top 10 + cross-check -------------------------------------------
    print("\n[5] Top 10 teams")
    print(f"    {'#':>3}  {'TEAM':<22} {'CODE':<5} {'POINTS':>9}")
    for entry in release.top(10):
        print(f"    {entry.rank:>3}  {entry.name:<22} {entry.country_code:<5} "
              f"{entry.points:>9.2f}")

    print("\n    CROSS-CHECK against screenshot:")
    france = release.lookup_by_code("FRA")
    spain = release.lookup_by_code("ESP")
    assert france is not None and france.rank == 1 and france.points == 1877.32, \
        f"France mismatch: {france}"
    assert spain is not None and spain.rank == 2 and spain.points == 1876.40, \
        f"Spain mismatch: {spain}"
    print(f"      France: #{france.rank} @ {france.points}  OK")
    print(f"      Spain:  #{spain.rank} @ {spain.points}  OK")

    # -- 6. lookup_by_code("FRA") ------------------------------------------
    print("\n[6] lookup_by_code('FRA')")
    print(f"    -> {france.name} #{france.rank} @ {france.points}")

    # -- 7. lookup_by_code("XXX") — expect None ----------------------------
    print("\n[7] lookup_by_code('XXX') — expect None")
    missing = release.lookup_by_code("XXX")
    print(f"    -> {missing}")
    assert missing is None, "expected None for bogus code"

    # -- 8. lookup_by_name("Côte d'Ivoire") --------------------------------
    print("\n[8] lookup_by_name(\"Côte d'Ivoire\")")
    civ = release.lookup_by_name("Côte d'Ivoire")
    if civ is not None:
        print(f"    -> {civ.name} ({civ.country_code}) #{civ.rank} @ {civ.points}")
    else:
        print("    -> not present in this release (no failure; informational)")

    print("\n" + "=" * 70)
    print("ALL CHECKS PASSED")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(f"\n[ASSERTION FAILED] {exc}")
        raise SystemExit(1)
    except FifaRankingsError as exc:
        print(f"\n[FIFA ERROR] {exc}")
        raise SystemExit(1)
