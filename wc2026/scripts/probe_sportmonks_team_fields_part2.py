"""PHASE A.2-A.6 (exploratory follow-up): resolve the cases Part 1 missed.

Part 1 left two teams unresolved (USA, Côte d'Ivoire) due to search-term
quirks, and never confirmed the UK-nation case (England/Wales) where ISO3 and
FIFA code diverge. This script resolves all four robustly, re-confirms the raw
shape of ``country.fifa_name``, rebuilds the 10-team comparison table, appends
to the shared findings file, and prints a join-strategy verdict.

Diagnostic only — does NOT create src/team_mapping.py. Run from project root:

    python scripts/probe_sportmonks_team_fields_part2.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote

# Allow standalone execution.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import SPORTMONKS_BASE_URL  # noqa: E402
from src.fifa_rankings import FifaRankingsClient, FifaRankingsError  # noqa: E402
from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
FINDINGS_PATH = _PROJECT_ROOT / "data" / "sportmonks_team_field_probe.json"

_CORE_BASE_URL = SPORTMONKS_BASE_URL.replace("/football", "/core")

# (label, resolution attempts). Each attempt is (method_label, kind, value)
# where kind is "search" (GET /teams/search/{value}) or "country" (resolve the
# country id via core /countries, then football /teams?filters=teamCountries).
TEAM_PLANS: list[tuple[str, list[tuple[str, str, str]]]] = [
    ("France", [("search:France", "search", "France")]),
    ("Argentina", [("search:Argentina", "search", "Argentina")]),
    ("Korea Republic", [("search:Korea Republic", "search", "Korea Republic")]),
    ("Türkiye", [("search:Türkiye", "search", "Türkiye")]),
    ("Iran", [("search:Iran", "search", "Iran")]),
    ("Mexico", [("search:Mexico", "search", "Mexico")]),
    ("United States", [
        ("search:United States", "search", "United States"),
        ("search:USA", "search", "USA"),
        ("country:United States", "country", "United States"),
    ]),
    ("Côte d'Ivoire", [
        ("search:Cote d'Ivoire", "search", "Cote d'Ivoire"),
        ("search:Ivory Coast", "search", "Ivory Coast"),
        ("search:Cote", "search", "Cote"),
        ("country:Ivory Coast", "country", "Ivory Coast"),
    ]),
    ("England", [("search:England", "search", "England")]),
    ("Wales", [("search:Wales", "search", "Wales")]),
]

# Which labels get a full raw JSON dump (the previously-failed/unconfirmed set,
# plus France for the A.3 sanity check). The other originals just fill a row.
FULL_DUMP_LABELS = {"France", "United States", "Côte d'Ivoire", "England", "Wales"}


def search_hits(client: SportmonksClient, term: str) -> list[dict[str, Any]]:
    encoded = quote(term)  # space -> %20, apostrophe -> %27, accents -> UTF-8
    resp = client.get(f"teams/search/{encoded}")
    data = resp.get("data")
    return data if isinstance(data, list) else []


def pick_national(hits: list[dict[str, Any]]) -> dict[str, Any] | None:
    nationals = [
        h for h in hits
        if h.get("type") == "national" and h.get("gender") == "male"
    ]
    if nationals:
        return nationals[0]
    return hits[0] if hits else None


def country_fallback(
    football: SportmonksClient, core: SportmonksClient, name: str
) -> dict[str, Any] | None:
    """Resolve a national team via country id (core /countries -> /teams)."""
    cresp = core.get(f"countries/search/{quote(name)}")
    countries = cresp.get("data")
    if not isinstance(countries, list) or not countries:
        return None
    country_id = countries[0].get("id")
    tresp = football.get(
        "teams",
        params={"filters": f"teamCountries:{country_id}", "include": "country",
                "per_page": 50},
    )
    teams = tresp.get("data")
    if not isinstance(teams, list):
        return None
    return pick_national(teams)


def fetch_team(client: SportmonksClient, team_id: int) -> dict[str, Any]:
    resp = client.get(f"teams/{team_id}", params={"include": "country"})
    data = resp.get("data")
    return data if isinstance(data, dict) else {}


def resolve_team(
    football: SportmonksClient,
    core: SportmonksClient,
    attempts: list[tuple[str, str, str]],
) -> tuple[dict[str, Any] | None, str | None]:
    """Try each attempt in order; return (full_team_obj, method_label)."""
    for label, kind, value in attempts:
        try:
            if kind == "search":
                picked = pick_national(search_hits(football, value))
            else:
                picked = country_fallback(football, core, value)
        except SportmonksError as exc:
            print(f"    [{label}] ERROR: {exc}")
            continue
        if picked and picked.get("id"):
            full = fetch_team(football, picked["id"])
            print(f"    [{label}] HIT -> id={picked['id']} name={full.get('name')!r}")
            return full, label
        print(f"    [{label}] no result")
    return None, None


def classify_join(release: Any, team: dict[str, Any]) -> dict[str, Any]:
    """Determine which join key resolves this team to a FIFA entry."""
    country = team.get("country") or {}
    fifa_name = country.get("fifa_name")
    short_code = team.get("short_code")
    iso3 = country.get("iso3")

    primary = release.lookup_by_code(fifa_name) if fifa_name else None
    fb1 = release.lookup_by_code(short_code) if short_code else None
    fb2 = release.lookup_by_code(iso3) if iso3 else None

    if primary is not None:
        sufficiency, entry = "primary (country.fifa_name)", primary
    elif fb1 is not None:
        sufficiency, entry = "fallback 1 (short_code)", fb1
    elif fb2 is not None:
        sufficiency, entry = "fallback 2 (country.iso3)", fb2
    else:
        sufficiency, entry = "OVERRIDE NEEDED", None

    return {
        "sm_name": team.get("name"),
        "short_code": short_code,
        "iso3": iso3,
        "fifa_name": fifa_name,
        "fifa_code": entry.country_code if entry else None,
        "fifa_display_name": entry.name if entry else None,
        "rank": entry.rank if entry else None,
        "points": entry.points if entry else None,
        "sufficiency": sufficiency,
        "primary_sufficient": primary is not None,
    }


def main() -> int:
    print("=" * 84)
    print("PHASE A.2-A.6 — RESOLVE FAILED/UNCONFIRMED TEAMS")
    print("=" * 84)

    football = SportmonksClient()
    core = SportmonksClient(base_url=_CORE_BASE_URL)

    print(f"\nCore API base: {_CORE_BASE_URL}")
    print("Resolving latest FIFA release...")
    try:
        release = FifaRankingsClient().fetch_latest()
        print(f"  FIFA {release.release_date} ({release.date_id}), {len(release)} teams.")
    except FifaRankingsError as exc:
        print(f"  [FIFA ERROR] {exc}")
        return 1

    results: list[dict[str, Any]] = []
    france_team: dict[str, Any] | None = None

    for label, attempts in TEAM_PLANS:
        print("\n" + "-" * 84)
        print(f"RESOLVE: {label}")
        print("-" * 84)
        team, method = resolve_team(football, core, attempts)
        if team is None:
            print(f"  *** UNRESOLVED: {label} (all attempts failed) ***")
            results.append({"label": label, "resolved": False})
            continue

        if label == "France":
            france_team = team

        if label in FULL_DUMP_LABELS:
            print("\n  FULL TEAM OBJECT (team + nested country):")
            print(_indent(json.dumps(team, indent=2, ensure_ascii=False)))

        info = classify_join(release, team)
        info.update({"label": label, "resolved": True,
                     "resolved_via": method, "team_id": team.get("id")})
        results.append(info)
        print(f"\n  JOIN: {info['sufficiency']} -> "
              f"FIFA {info['fifa_display_name']} [{info['fifa_code']}] "
              f"#{info['rank']} @ {info['points']}")

    # -- A.3 explicit confirmation ----------------------------------------
    print("\n" + "=" * 84)
    print("A.3 — RAW country.fifa_name CONFIRMATION (France)")
    print("=" * 84)
    if france_team:
        fn = (france_team.get("country") or {}).get("fifa_name")
        print(f"  country.fifa_name = {fn!r}  "
              f"(type={type(fn).__name__}, len={len(fn) if isinstance(fn, str) else 'n/a'})")
        print("  -> Confirmed: a 3-letter uppercase code, not a display name."
              if isinstance(fn, str) and len(fn) == 3 and fn.isupper()
              else "  -> UNEXPECTED shape; review the full dump above.")

    # -- A.4 expanded table -----------------------------------------------
    print("\n" + "=" * 84)
    print("A.4 — EXPANDED COMPARISON TABLE")
    print("=" * 84)
    print(f"{'SM Team Name':<16} | {'short_code':<10} | {'iso3':<5} | "
          f"{'fifa_name':<9} | {'FIFA Code':<9} | FIFA Name")
    print("-" * 84)
    for r in results:
        if not r.get("resolved"):
            print(f"{r['label']:<16} | {'(unresolved)'}")
            continue
        print(f"{str(r['sm_name']):<16} | {str(r['short_code']):<10} | "
              f"{str(r['iso3']):<5} | {str(r['fifa_name']):<9} | "
              f"{str(r['fifa_code']):<9} | {r['fifa_display_name']}")

    # -- A.6 verdict -------------------------------------------------------
    print("\n" + "=" * 84)
    print("A.6 — VERDICT: PROPOSED JOIN STRATEGY")
    print("=" * 84)
    print("  Primary key : country.fifa_name      -> FIFA ranking.country_code")
    print("  Fallback 1  : team.short_code        -> FIFA country_code")
    print("  Fallback 2  : country.iso3           -> FIFA country_code")
    print("  Fallback 3  : manual override file keyed by SportMonks team_id")
    print("\n  Per-team key sufficiency:")
    all_primary = True
    for r in results:
        if not r.get("resolved"):
            all_primary = False
            print(f"    - {r['label']:<16}: UNRESOLVED")
            continue
        if not r["primary_sufficient"]:
            all_primary = False
        flag = "primary OK" if r["primary_sufficient"] else f"NEEDS {r['sufficiency']}"
        print(f"    - {r['sm_name']:<16}: {flag} "
              f"(fifa_name={r['fifa_name']}, iso3={r['iso3']})")

    print("\n  CONCLUSION:")
    if all_primary:
        print("    country.fifa_name is reliably present and matches FIFA "
              "country_code for")
        print("    all 10 teams. The override file can START EMPTY — kept ready "
              "for surprises.")
    else:
        print("    Some teams needed a fallback (see above) — the override file "
              "should seed those.")

    # -- A.5 append findings ----------------------------------------------
    existing: dict[str, Any] = {}
    if FINDINGS_PATH.exists():
        try:
            existing = json.loads(FINDINGS_PATH.read_text(encoding="utf-8"))
        except ValueError:
            existing = {}
    existing["part2"] = {
        "fifa_release": release.release_date,
        "core_base_url": _CORE_BASE_URL,
        "all_primary_sufficient": all_primary,
        "teams": results,
    }
    FINDINGS_PATH.write_text(
        json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\n  Appended Part 2 results to {FINDINGS_PATH}")
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
