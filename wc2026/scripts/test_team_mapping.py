"""End-to-end test of src/team_mapping.py against live data.

Resolves 10 World Cup teams through TeamFifaMapper, asserts every one hits the
primary key (country.fifa_name), spot-checks the Wales edge case, and exercises
the error and override paths.

Exits non-zero if any of the 10 fails to resolve, if any resolves via a
fallback (on our verified data they should all hit primary), or if Wales does
not resolve via "fifa_name" to WAL.

Run from the project root:

    python scripts/test_team_mapping.py
"""

from __future__ import annotations

import json
import logging
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.fifa_rankings import get_current_rankings  # noqa: E402
from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402
from src.team_mapping import (  # noqa: E402
    TeamFifaMapper,
    TeamFifaMapping,
    UnresolvedTeam,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

# Search terms verified to resolve in the field probe (note "Cote" for CIV and
# "United States" for USA).
TEAM_SEARCH_TERMS = [
    "France",
    "Argentina",
    "Korea Republic",
    "Türkiye",
    "Iran",
    "Mexico",
    "United States",
    "Cote",
    "England",
    "Wales",
]


def resolve_team_object(client: SportmonksClient, term: str) -> dict[str, Any] | None:
    resp = client.get(f"teams/search/{term}")
    hits = resp.get("data")
    if not isinstance(hits, list) or not hits:
        return None
    nationals = [h for h in hits if h.get("type") == "national" and h.get("gender") == "male"]
    picked = nationals[0] if nationals else hits[0]
    full = client.get(f"teams/{picked['id']}", params={"include": "country"})
    data = full.get("data")
    return data if isinstance(data, dict) else None


def main() -> int:
    print("=" * 78)
    print("TEAM MAPPING — END-TO-END TEST")
    print("=" * 78)

    release = get_current_rankings()
    print(f"FIFA release {release.release_date} ({release.date_id}), "
          f"{len(release)} teams.\n")

    mapper = TeamFifaMapper(release)
    sm = SportmonksClient()

    failures: list[str] = []
    fallback_used: list[str] = []
    resolved_count = 0
    wales_mapping: TeamFifaMapping | None = None

    print(f"{'TEAM (SM)':<18} {'ID':>7}  {'METHOD':<12} {'FIFA':<5} {'POINTS':>9}")
    print("-" * 78)
    for term in TEAM_SEARCH_TERMS:
        team = resolve_team_object(sm, term)
        if team is None:
            failures.append(f"{term}: SportMonks search returned nothing")
            print(f"{term:<18} {'?':>7}  SEARCH FAILED")
            continue

        result = mapper.resolve(team)
        if isinstance(result, UnresolvedTeam):
            failures.append(f"{result.sportmonks_name} (id={result.sportmonks_team_id})")
            print(f"{result.sportmonks_name:<18} {result.sportmonks_team_id:>7}  UNRESOLVED")
            continue

        resolved_count += 1
        if result.resolution_method != "fifa_name":
            fallback_used.append(f"{result.sportmonks_name} -> {result.resolution_method}")
        if result.sportmonks_name == "Wales":
            wales_mapping = result

        print(f"{result.sportmonks_name:<18} {result.sportmonks_team_id:>7}  "
              f"{result.resolution_method:<12} {result.fifa_country_code:<5} "
              f"{result.fifa_points:>9.2f}")

    # -- Wales edge-case check --------------------------------------------
    print("\n[Wales check] must resolve via 'fifa_name' to WAL (NOT iso3=WLS)")
    if wales_mapping is None:
        failures.append("Wales did not resolve at all")
    else:
        ok = (wales_mapping.resolution_method == "fifa_name"
              and wales_mapping.fifa_country_code == "WAL")
        print(f"  method={wales_mapping.resolution_method} "
              f"code={wales_mapping.fifa_country_code} "
              f"points={wales_mapping.fifa_points}  -> {'OK' if ok else 'FAIL'}")
        if not ok:
            failures.append("Wales did not resolve via fifa_name -> WAL")

    # -- Error path -------------------------------------------------------
    print("\n[Error path] fake team with invalid codes -> expect UnresolvedTeam")
    fake_team = {
        "id": 999999,
        "name": "Faketopia",
        "short_code": "XXX",
        "country": {"fifa_name": "XXX", "iso3": "XXX"},
    }
    fake_result = mapper.resolve(fake_team)
    if isinstance(fake_result, UnresolvedTeam):
        print(f"  UnresolvedTeam OK; attempted_keys={fake_result.attempted_keys}")
        assert fake_result.attempted_keys.get("fifa_name") == "XXX"
        assert fake_result.attempted_keys.get("short_code") == "XXX"
        assert fake_result.attempted_keys.get("iso3") == "XXX"
    else:
        failures.append("Fake team unexpectedly resolved")
        print("  FAIL: fake team resolved")

    # -- Override path ----------------------------------------------------
    print("\n[Override path] override France (id=18647) -> BRA, expect priority")
    with tempfile.TemporaryDirectory() as tmp:
        ov_path = Path(tmp) / "overrides.json"
        ov_path.write_text(json.dumps({
            "overrides": {
                "18647": {"fifa_country_code": "BRA", "reason": "test override"}
            }
        }), encoding="utf-8")

        ov_mapper = TeamFifaMapper(release, overrides_path=ov_path)
        france = resolve_team_object(sm, "France")
        ov_result = ov_mapper.resolve(france)
        if (isinstance(ov_result, TeamFifaMapping)
                and ov_result.resolution_method == "override"
                and ov_result.fifa_country_code == "BRA"):
            print(f"  override OK -> {ov_result.fifa_name} [{ov_result.fifa_country_code}] "
                  f"@ {ov_result.fifa_points} (method={ov_result.resolution_method})")
        else:
            failures.append("Override did not take priority")
            print(f"  FAIL: {ov_result}")

    # Confirm override is gone once the temp mapper is discarded.
    post = mapper.resolve(resolve_team_object(sm, "France"))
    assert isinstance(post, TeamFifaMapping) and post.fifa_country_code == "FRA", \
        "France should map to FRA again after override removed"
    print("  override removed; France maps to FRA again.")

    # -- Summary ----------------------------------------------------------
    print("\n" + "=" * 78)
    print("SUMMARY")
    print("=" * 78)
    print(f"  Resolved: {resolved_count}/{len(TEAM_SEARCH_TERMS)}")
    print(f"  Unresolved/failures: {len(failures)}")
    print(f"  Fallback methods used: {fallback_used or 'none (all primary)'}")

    if failures:
        print("\n  FAILURES:")
        for f in failures:
            print(f"    - {f}")
        return 1
    if fallback_used:
        print("\n  FAIL: a team resolved via fallback (expected all primary).")
        return 1

    print("\nALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(f"\n[ASSERTION FAILED] {exc}")
        raise SystemExit(1)
    except SportmonksError as exc:
        print(f"\n[SPORTMONKS ERROR] {exc}")
        raise SystemExit(1)
