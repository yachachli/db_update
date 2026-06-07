"""PHASE A (exploratory): probe what SportMonks exposes on team objects.

Goal: see exactly which identifiers SportMonks gives us on a team (name,
short_code, country_id, and the nested country's name/iso2/iso3) so we can
design src/team_mapping.py — the bridge from a SportMonks team to a FIFA
ranking entry (and thus to opponent_fifa_points).

This is diagnostic only. It does NOT create the mapping module or override
file. Run from the project root:

    python scripts/probe_sportmonks_team_fields.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

# Allow standalone execution.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.fifa_rankings import FifaRankingsClient, FifaRankingsError  # noqa: E402
from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
FINDINGS_PATH = _PROJECT_ROOT / "data" / "sportmonks_team_field_probe.json"

# 8 deliberately diverse teams. Several search terms stress-test tricky cases:
# accents/apostrophes (Côte d'Ivoire), abbreviation (USA), FIFA naming
# (Korea Republic), and the Turkish rename (Türkiye).
SEARCH_TERMS = [
    "France",
    "Argentina",
    "Côte d'Ivoire",
    "USA",
    "Korea Republic",
    "Türkiye",
    "Iran",
    "Mexico",
]


def search_team(client: SportmonksClient, term: str) -> dict[str, Any] | None:
    """Return the first /teams/search hit for ``term`` (or None)."""
    try:
        resp = client.get(f"teams/search/{term}")
    except SportmonksError as exc:
        print(f"  [SEARCH ERROR] '{term}': {exc}")
        return None
    data = resp.get("data")
    if not isinstance(data, list) or not data:
        print(f"  [NO RESULTS] '{term}'")
        return None
    return data[0]


def fetch_team(client: SportmonksClient, team_id: int) -> dict[str, Any]:
    """Fetch the full team object with the nested country included."""
    resp = client.get(f"teams/{team_id}", params={"include": "country"})
    data = resp.get("data")
    return data if isinstance(data, dict) else {}


def fifa_match(
    release: Any, iso3: str | None, name: str | None
) -> tuple[Any, str]:
    """Try to find the FIFA entry, returning (entry_or_None, how_matched)."""
    if iso3:
        entry = release.lookup_by_code(iso3)
        if entry is not None:
            return entry, "code"
    if name:
        entry = release.lookup_by_name(name)
        if entry is not None:
            return entry, "name"
    return None, "none"


def main() -> int:
    print("=" * 78)
    print("PHASE A — SPORTMONKS TEAM FIELD PROBE")
    print("=" * 78)

    sm = SportmonksClient()

    print("\nResolving the latest FIFA release for cross-referencing...")
    try:
        release = FifaRankingsClient().fetch_latest()
        print(f"  FIFA release {release.release_date} ({release.date_id}), "
              f"{len(release)} teams.")
    except FifaRankingsError as exc:
        print(f"  [FIFA ERROR] could not load rankings: {exc}")
        return 1

    findings: list[dict[str, Any]] = []
    table_rows: list[tuple[str, str, str, str, str]] = []
    mapping_needed: list[dict[str, Any]] = []

    for term in SEARCH_TERMS:
        print("\n" + "-" * 78)
        print(f"SEARCH: '{term}'")
        print("-" * 78)

        hit = search_team(sm, term)
        if hit is None:
            findings.append({"search_term": term, "resolved": False})
            mapping_needed.append({"search_term": term, "reason": "no search result"})
            continue

        team_id = hit.get("id")
        print(f"  Resolved team id={team_id} name={hit.get('name')!r}")

        team = fetch_team(sm, team_id)
        country = team.get("country") or {}

        # Full JSON dump of the team object (including nested country).
        print("\n  FULL TEAM OBJECT:")
        print(_indent(json.dumps(team, indent=2, ensure_ascii=False)))

        sm_name = team.get("name")
        sm_short = team.get("short_code")
        sm_iso3 = country.get("iso3")
        sm_iso2 = country.get("iso2")
        sm_country_name = country.get("name")

        entry, how = fifa_match(release, sm_iso3, sm_name)
        fifa_code = entry.country_code if entry else None
        fifa_name = entry.name if entry else None
        codes_match = bool(entry and sm_iso3 and fifa_code and sm_iso3.upper() == fifa_code.upper())

        record = {
            "search_term": term,
            "resolved": True,
            "sportmonks": {
                "id": team_id,
                "name": sm_name,
                "short_code": sm_short,
                "country_id": team.get("country_id"),
                "country_name": sm_country_name,
                "country_iso2": sm_iso2,
                "country_iso3": sm_iso3,
            },
            "fifa": {
                "matched": entry is not None,
                "matched_by": how,
                "code": fifa_code,
                "name": fifa_name,
                "rank": entry.rank if entry else None,
                "points": entry.points if entry else None,
            },
            "codes_match": codes_match,
        }
        findings.append(record)
        table_rows.append((
            str(sm_name), str(sm_short), str(sm_iso3),
            str(fifa_code), str(fifa_name),
        ))

        if entry is None or not codes_match:
            reason = "no FIFA match" if entry is None else "code mismatch"
            print(f"\n  *** MAPPING NEEDED ({reason}) ***")
            print(f"      SportMonks: name={sm_name!r} short_code={sm_short!r} "
                  f"iso3={sm_iso3!r} iso2={sm_iso2!r} country={sm_country_name!r}")
            print(f"      FIFA:       matched_by={how} code={fifa_code!r} "
                  f"name={fifa_name!r}")
            mapping_needed.append(record)
        else:
            print(f"\n  OK: {sm_name} [{sm_iso3}] -> FIFA {fifa_name} [{fifa_code}] "
                  f"#{entry.rank} @ {entry.points} (matched by {how})")

    # -- Comparison table --------------------------------------------------
    print("\n" + "=" * 78)
    print("COMPARISON TABLE")
    print("=" * 78)
    header = ("SM Team Name", "SM Short", "SM ISO3", "FIFA Code", "FIFA Name")
    print(f"{header[0]:<22} | {header[1]:<8} | {header[2]:<7} | "
          f"{header[3]:<9} | {header[4]}")
    print("-" * 78)
    for row in table_rows:
        print(f"{row[0]:<22} | {row[1]:<8} | {row[2]:<7} | {row[3]:<9} | {row[4]}")

    # -- Mapping-needed summary --------------------------------------------
    print("\n" + "=" * 78)
    print(f"MAPPING NEEDED: {len(mapping_needed)} team(s)")
    print("=" * 78)
    for rec in mapping_needed:
        if rec.get("resolved"):
            sm = rec["sportmonks"]
            fifa = rec["fifa"]
            print(f"  - {sm['name']}: SM(iso3={sm['country_iso3']}, "
                  f"short={sm['short_code']}) vs FIFA(code={fifa['code']}, "
                  f"name={fifa['name']})")
        else:
            print(f"  - {rec['search_term']}: {rec.get('reason')}")
    if not mapping_needed:
        print("  (none — every team matched FIFA by code)")

    FINDINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    FINDINGS_PATH.write_text(
        json.dumps(
            {"fifa_release": release.release_date, "teams": findings,
             "mapping_needed": mapping_needed},
            indent=2, ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(f"\nSaved findings to {FINDINGS_PATH}")
    return 0


def _indent(text: str, spaces: int = 4) -> str:
    pad = " " * spaces
    return "\n".join(pad + line for line in text.splitlines())


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SportmonksError as exc:
        print(f"\n[SPORTMONKS ERROR] {exc}")
        raise SystemExit(1)
