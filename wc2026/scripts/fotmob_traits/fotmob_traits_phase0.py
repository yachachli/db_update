"""Phase 0 — live FotMob API discovery for player traits scraper.

Run:
    py -3 scripts/fotmob_traits/fotmob_traits_phase0.py

Confirms auth, search JSON shape, and traits block path against live responses.
STOP after this script — do not build the full scraper until findings are confirmed.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fotmob_auth import build_x_mas_header  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "fotmob_cache" / "phase0"
OUT_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://www.fotmob.com"
SEARCH_PATH = "/api/data/search/suggest"
PLAYER_DATA_PATH = "/api/data/playerData"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.fotmob.com/",
}

MARQUEE = [
    "Kylian Mbappe",
    "Jude Bellingham",
    "Mohamed Salah",
    "Lionel Messi",
    "Erling Haaland",
]


def _save(name: str, payload: Any) -> Path:
    path = OUT_DIR / name
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def _signed_get(session: requests.Session, api_path: str) -> requests.Response:
    headers = {
        **BROWSER_HEADERS,
        "x-mas": build_x_mas_header(api_path),
        "Content-Type": "application/json",
    }
    time.sleep(0.35)
    return session.get(BASE + api_path, headers=headers, timeout=30)


def _unsigned_get(session: requests.Session, api_path: str) -> requests.Response:
    time.sleep(0.35)
    return session.get(BASE + api_path, headers=BROWSER_HEADERS, timeout=30)


def parse_search_players(body: Any) -> list[dict[str, Any]]:
    players: list[dict[str, Any]] = []
    if not isinstance(body, list):
        return players
    for group in body:
        for hit in group.get("suggestions") or []:
            if hit.get("type") == "player":
                players.append(
                    {
                        "id": int(hit["id"]),
                        "name": hit.get("name"),
                        "team": hit.get("teamName"),
                        "team_id": hit.get("teamId"),
                        "score": hit.get("score"),
                    }
                )
    return players


def extract_traits(player_body: dict[str, Any]) -> dict[str, Any] | None:
    traits = player_body.get("traits")
    if not isinstance(traits, dict) or not traits.get("items"):
        return None
    return traits


def main() -> int:
    session = requests.Session()
    report: dict[str, Any] = {"auth": {}, "search": {}, "traits": {}, "marquee": []}

    print("=" * 72)
    print("FotMob Phase 0 — live API discovery")
    print("=" * 72)

    # --- 1. Auth gate ---
    print("\n[1] Auth")
    legacy_path = "/api/playerData?id=737066"
    signed_path = "/api/data/playerData?id=737066"

    legacy = _unsigned_get(session, legacy_path)
    print(f"  unsigned {legacy_path}: HTTP {legacy.status_code}")

    signed = _signed_get(session, signed_path)
    print(f"  signed   {signed_path}: HTTP {signed.status_code}")

    report["auth"]["unsigned_legacy_path"] = {
        "path": legacy_path,
        "status": legacy.status_code,
        "note": "Returns FotMob HTML shell (404) — endpoint moved / auth required",
    }
    report["auth"]["signed_current_path"] = {
        "path": signed_path,
        "status": signed.status_code,
        "scheme": "x-mas (MD5 of JSON body + Three Lions lyrics, base64 token)",
    }

    if signed.status_code == 403:
        print("  BLOCKED: 403 — x-mas signing scheme may have changed.")
        report["auth"]["gate"] = "FAIL_403"
        _save("phase0_report.json", report)
        return 1
    if signed.status_code != 200:
        print(f"  BLOCKED: signed playerData returned HTTP {signed.status_code}")
        report["auth"]["gate"] = f"FAIL_{signed.status_code}"
        _save("phase0_report.json", report)
        return 1

    pdata = signed.json()
    if not isinstance(pdata, dict) or not pdata.get("name"):
        print("  BLOCKED: signed playerData did not return a player object")
        report["auth"]["gate"] = "FAIL_EMPTY_BODY"
        _save("phase0_report.json", report)
        return 1

    print(f"  OK: signed playerData returned {pdata.get('name')!r}")
    report["auth"]["gate"] = "PASS"
    _save("sample_playerData_haaland.json", pdata)

    # --- 2. Search shape ---
    print("\n[2] Search endpoint")
    report["search"]["endpoint"] = SEARCH_PATH
    report["search"]["full_url_pattern"] = (
        "https://www.fotmob.com/api/data/search/suggest?term={urlencoded_name}"
    )
    report["search"]["response_shape"] = (
        "list[group] -> group.suggestions[] where type=='player'; "
        "fields: id, name, teamName, teamId, score"
    )
    report["search"]["disambiguation_note"] = (
        "No country/cc in search hits — use teamName (+ manual overrides) "
        "to avoid false positives (e.g. multiple Mohamed Salah)."
    )

    search_sample = _signed_get(
        session, f"{SEARCH_PATH}?term={urllib.parse.quote('Lionel Messi')}"
    )
    print(f"  signed search: HTTP {search_sample.status_code}")
    if search_sample.status_code == 200:
        body = search_sample.json()
        _save("sample_search_messi.json", body)
        cands = parse_search_players(body)
        report["search"]["sample_candidates"] = cands[:5]
        for c in cands[:3]:
            print(f"    id={c['id']} name={c['name']!r} team={c['team']!r}")

    # --- 3. Traits path ---
    print("\n[3] Traits block")
    traits = extract_traits(pdata)
    if traits:
        report["traits"]["json_path"] = "playerData.traits"
        report["traits"]["fields"] = {
            "key": "traits.key (position bucket, e.g. stats_comparison_forwards)",
            "title": "traits.title (e.g. 'Stats compared to other forwards')",
            "items": "traits.items[] with {key, title, value}",
            "value_range": "value is 0.0–1.0 float; multiply by 100 for display %",
        }
        report["traits"]["sample"] = traits
        print(f"  path: playerData.traits")
        print(f"  title: {traits.get('title')!r}")
        for item in traits.get("items", [])[:6]:
            pct = int(round(float(item["value"]) * 100))
            print(f"    {item['title']}: {pct}% (raw={item['value']})")
    else:
        print("  WARNING: no traits on sample player")

    # --- 4. Marquee spot-check ---
    print("\n[4] Marquee spot-check (5 players)")
    for name in MARQUEE:
        path = f"{SEARCH_PATH}?term={urllib.parse.quote(name)}"
        sresp = _signed_get(session, path)
        entry: dict[str, Any] = {"query": name, "search_status": sresp.status_code}
        if sresp.status_code != 200:
            report["marquee"].append(entry)
            print(f"  {name}: search HTTP {sresp.status_code}")
            continue
        cands = parse_search_players(sresp.json())
        if not cands:
            entry["error"] = "no_candidates"
            report["marquee"].append(entry)
            print(f"  {name}: no candidates")
            continue
        top = cands[0]
        pdata_resp = _signed_get(session, f"{PLAYER_DATA_PATH}?id={top['id']}")
        if pdata_resp.status_code != 200:
            entry["error"] = f"playerData HTTP {pdata_resp.status_code}"
            report["marquee"].append(entry)
            continue
        body = pdata_resp.json()
        t = extract_traits(body) if isinstance(body, dict) else None
        entry["resolved"] = {
            "fotmob_id": top["id"],
            "name": body.get("name") if isinstance(body, dict) else None,
            "team": top["team"],
            "has_traits": bool(t),
            "compared_to": t.get("title") if t else None,
            "goals_pct": (
                int(round(float(next(
                    i for i in t["items"] if i["key"] == "goals"
                )["value"]) * 100))
                if t
                else None
            ),
        }
        report["marquee"].append(entry)
        print(
            f"  {name} -> id={top['id']} {entry['resolved']['name']!r} "
            f"traits={entry['resolved']['has_traits']} "
            f"goals%={entry['resolved']['goals_pct']}"
        )

    _save("phase0_report.json", report)

    print("\n" + "=" * 72)
    print("PHASE 0 COMPLETE — confirm findings before building the scraper.")
    print("=" * 72)
    print(f"Report: {OUT_DIR / 'phase0_report.json'}")
    print("\nKey findings:")
    print("  • Auth: x-mas header required; endpoints live under /api/data/...")
    print("  • Search: /api/data/search/suggest?term=...")
    print("  • Traits: playerData.traits.items[].{key,title,value}")
    print("  • Percentiles: value * 100 (0.99 -> 99%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
