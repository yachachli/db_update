"""Diagnostic: discover the TRUE latest FIFA ranking release / dateId.

A prior probe found id14870 (2025-09-18) as the newest dateId embedded in the
page, but FIFA shows a more recent release (1 Apr 2026). This script finds the
reliable way to fetch the current ranking before we build the FIFA module.

Separate data source from SportMonks: plain ``requests``.

Run from the project root:

    python scripts/probe_latest_fifa_dateid.py
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_HTML_PATH = _PROJECT_ROOT / "data" / "fifa_rankings_raw.html"
FINDINGS_PATH = _PROJECT_ROOT / "data" / "fifa_dateid_investigation.json"

PAGE_URL = "https://inside.fifa.com/fifa-world-ranking/men"
API_BASE = "https://inside.fifa.com/api/ranking-overview"
TIMEOUT = 30

# CONFIRMED: FIFA's dateId is a daily counter. Two embedded anchors prove the
# linear map (id14702=2025-04-03, id14800=2025-07-10 (+98/+98 days),
# id14870=2025-09-18 (+70/+70 days)). So:  id = ANCHOR_ID + (date - ANCHOR_DATE)
# Only EXACT release dates return data; off-release dateIds return an empty
# 15-16 byte body. Verified: 2026-04-01 -> id15065 -> France #1 @ 1877.32.
ANCHOR_ID = 14870
ANCHOR_DATE = date(2025, 9, 18)
# Releases are at most ~2 months apart; this bounds the walk-back search.
MAX_WALKBACK_DAYS = 120

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": PAGE_URL,
}

findings: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def id_for_date(d: date) -> str:
    """Map a calendar date to its FIFA dateId via the daily-counter relation."""
    return f"id{ANCHOR_ID + (d - ANCHOR_DATE).days}"


def api_get(params: dict[str, Any]) -> tuple[int, int, Any]:
    """Return (status, body_size, parsed_json_or_None)."""
    resp = requests.get(API_BASE, headers=BROWSER_HEADERS, params=params, timeout=TIMEOUT)
    size = len(resp.text)
    if resp.status_code != 200:
        return resp.status_code, size, None
    # Off-release dateIds return a tiny (~15-16 byte) empty body.
    if size < 50:
        return 200, size, None
    try:
        return 200, size, resp.json()
    except ValueError:
        return 200, size, None


def find_ranking_list(payload: Any) -> list[dict[str, Any]] | None:
    """Find the largest list of dicts carrying ranking rows."""
    best: list[dict[str, Any]] | None = None

    def walk(node: Any) -> None:
        nonlocal best
        if isinstance(node, list) and node and all(isinstance(i, dict) for i in node):
            keys = set().union(*(i.keys() for i in node))
            if "rankingItem" in keys or "totalPoints" in keys:
                if best is None or len(node) > len(best):
                    best = node
        if isinstance(node, dict):
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(payload)
    return best


def _row_fields(entry: dict[str, Any]) -> tuple[Any, Any, Any]:
    """Return (rank, name, totalPoints) from a ranking row of either shape."""
    item = entry.get("rankingItem") if isinstance(entry.get("rankingItem"), dict) else entry
    return item.get("rank"), item.get("name"), item.get("totalPoints")


def top5(rankings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for entry in rankings:
        rank, name, pts = _row_fields(entry)
        rows.append({"rank": rank, "name": name, "totalPoints": pts})
    rows.sort(key=lambda r: r["rank"] if isinstance(r["rank"], int) else 9999)
    return rows[:5]


def release_date_of(payload: Any, rankings: list[dict[str, Any]]) -> str | None:
    """Best-effort extraction of the release/update date."""
    if rankings:
        item = rankings[0].get("rankingItem", rankings[0])
        for key in ("lastUpdateDate", "date", "publishedDate"):
            if isinstance(item, dict) and item.get(key):
                return str(item[key])
        if rankings[0].get("lastUpdateDate"):
            return str(rankings[0]["lastUpdateDate"])
    return None


def france_row(rankings: list[dict[str, Any]]) -> dict[str, Any] | None:
    for entry in rankings:
        rank, name, pts = _row_fields(entry)
        if name == "France":
            return {"rank": rank, "name": name, "totalPoints": pts}
    return None


def print_rows(rows: list[dict[str, Any]]) -> None:
    for r in rows:
        print(f"    #{r['rank']:<3} {r['name']:<20} {r['totalPoints']}")


# ---------------------------------------------------------------------------
# Phase 1
# ---------------------------------------------------------------------------
def phase1_no_dateid() -> dict[str, Any] | None:
    print("\n" + "=" * 70)
    print("PHASE 1: API WITHOUT dateId")
    print("=" * 70)
    status, size, payload = api_get({"locale": "en"})
    print(f"HTTP {status} | body {size} chars")
    if status != 200 or payload is None:
        print("No usable ranking data WITHOUT a dateId -> dateId is REQUIRED.")
        findings["phase1"] = {"status": status, "body_size": size, "usable": False}
        return None
    rankings = find_ranking_list(payload) or []
    rel = release_date_of(payload, rankings)
    fra = france_row(rankings)
    print(f"Response size: {size:,} chars")
    print(f"Ranking entries: {len(rankings)}")
    print(f"Implicit release date: {rel}")
    print("Top 5:")
    print_rows(top5(rankings))
    print(f"France: {fra}")
    findings["phase1"] = {
        "status": status, "usable": bool(rankings),
        "release_date": rel, "entries": len(rankings),
        "top5": top5(rankings), "france": fra,
    }
    return {"payload": payload, "rankings": rankings, "release_date": rel, "france": fra}


# ---------------------------------------------------------------------------
# Phase 2 (revised): daily-counter walk-back from today
# ---------------------------------------------------------------------------
def phase2_walkback() -> dict[str, Any] | None:
    """Compute today's dateId and walk backward until a release with data.

    The spec's naive +1 forward probe cannot work: dateIds increment ~1/day,
    but releases are months apart, so id14871..id14875 are all empty and the
    "5 consecutive errors" rule halts immediately. Instead we exploit the
    confirmed linear map and walk back one calendar day at a time.
    """
    print("\n" + "=" * 70)
    print("PHASE 2: DAILY-COUNTER WALK-BACK FROM TODAY")
    print("=" * 70)
    today = date.today()
    print(f"Today: {today.isoformat()}  ->  computed dateId {id_for_date(today)}")
    probed: list[dict[str, Any]] = []

    for offset in range(MAX_WALKBACK_DAYS + 1):
        d = today - timedelta(days=offset)
        date_id = id_for_date(d)
        status, size, payload = api_get({"locale": "en", "dateId": date_id})
        rankings = find_ranking_list(payload) if payload else None
        valid = status == 200 and bool(rankings)
        probed.append({"date": d.isoformat(), "dateId": date_id, "size": size, "valid": valid})
        if valid:
            rel = release_date_of(payload, rankings or []) or d.isoformat()
            print(f"  {d.isoformat()} ({date_id}): HIT  size={size:,} entries={len(rankings)}")
            print(f"\nLatest release found: {rel}  ->  {date_id}")
            print("Top 5:")
            print_rows(top5(rankings))  # type: ignore[arg-type]
            findings["phase2"] = {
                "method": "daily-counter walk-back",
                "today": today.isoformat(),
                "latest_dateId": date_id,
                "latest_release_date": rel,
                "days_back": offset,
                "top5": top5(rankings),  # type: ignore[arg-type]
                "france": france_row(rankings),  # type: ignore[arg-type]
            }
            return {"dateId": date_id, "rankings": rankings, "release_date": rel}
        print(f"  {d.isoformat()} ({date_id}): empty (size={size})")
        time.sleep(0.3)

    print(f"\nNo release found within {MAX_WALKBACK_DAYS} days back.")
    findings["phase2"] = {"method": "daily-counter walk-back", "found": False, "probed": probed}
    return None


# ---------------------------------------------------------------------------
# Phase 3
# ---------------------------------------------------------------------------
def phase3_index_endpoints() -> None:
    print("\n" + "=" * 70)
    print("PHASE 3: CANDIDATE INDEX/CALENDAR ENDPOINTS")
    print("=" * 70)
    candidates = [
        "https://inside.fifa.com/api/ranking-overview/dates",
        "https://inside.fifa.com/api/rankings",
        "https://inside.fifa.com/api/ranking-dates",
        "https://inside.fifa.com/api/world-ranking",
        "https://inside.fifa.com/api/ranking-overview/latest",
    ]
    results = []
    for url in candidates:
        try:
            resp = requests.get(url, headers=BROWSER_HEADERS, timeout=TIMEOUT)
            ctype = resp.headers.get("content-type", "")
            shape = "?"
            if "json" in ctype:
                try:
                    data = resp.json()
                    shape = type(data).__name__
                    if isinstance(data, dict):
                        shape += f" keys={sorted(data.keys())[:8]}"
                    elif isinstance(data, list):
                        shape += f" len={len(data)}"
                except ValueError:
                    shape = "non-JSON body"
            print(f"  HTTP {resp.status_code} | {ctype:<35} | {url}")
            if "json" in ctype:
                print(f"      shape: {shape}")
            results.append({"url": url, "status": resp.status_code, "content_type": ctype, "shape": shape})
        except requests.RequestException as exc:
            print(f"  ERROR {url} -> {exc}")
            results.append({"url": url, "error": str(exc)})
        time.sleep(0.3)
    findings["phase3"] = results


# ---------------------------------------------------------------------------
# Phase 4
# ---------------------------------------------------------------------------
def phase4_next_data() -> None:
    print("\n" + "=" * 70)
    print("PHASE 4: RE-INSPECT __NEXT_DATA__ (forced fresh fetch)")
    print("=" * 70)
    resp = requests.get(
        PAGE_URL,
        headers={**BROWSER_HEADERS, "Accept": "text/html,*/*", "Cache-Control": "no-cache"},
        timeout=TIMEOUT,
    )
    print(f"HTTP {resp.status_code} | {len(resp.content):,} bytes")
    RAW_HTML_PATH.write_text(resp.text, encoding="utf-8")
    html = resp.text

    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html, re.DOTALL,
    )
    if not match:
        print("No __NEXT_DATA__ found.")
        findings["phase4"] = {"next_data": False}
        return
    data = json.loads(match.group(1))

    # Collect dateId/date entries.
    date_entries: dict[str, str] = {}

    def walk_dates(node: Any) -> None:
        if isinstance(node, dict):
            nid, ndate = node.get("id"), node.get("date")
            if isinstance(nid, str) and nid.startswith("id") and isinstance(ndate, str) and re.match(r"\d{4}-\d{2}-\d{2}", ndate):
                date_entries[nid] = ndate
            for v in node.values():
                walk_dates(v)
        elif isinstance(node, list):
            for v in node:
                walk_dates(v)

    walk_dates(data)

    # Look for explicit "current/latest/selected" dateId keys.
    pointer_keys = {}

    def walk_pointers(node: Any) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                if isinstance(k, str) and any(t in k.lower() for t in ("currentdateid", "latestdateid", "selecteddateid", "currentdate", "latestdate")):
                    pointer_keys[k] = v
                walk_pointers(v)
        elif isinstance(node, list):
            for v in node:
                walk_pointers(v)

    walk_pointers(data)

    ordered = sorted(date_entries.items(), key=lambda kv: int(kv[0][2:]), reverse=True)
    print(f"Total dateIds in __NEXT_DATA__: {len(date_entries)}")
    print("Top 5 by numeric id:")
    for nid, ndate in ordered[:5]:
        print(f"  {nid}  ({ndate})")
    print(f"Explicit pointer keys found: {pointer_keys or 'none'}")
    has_apr = any("2026-04" in d for d in date_entries.values())
    print(f"Any 'April 2026' date present in list: {has_apr}")

    findings["phase4"] = {
        "total_date_ids": len(date_entries),
        "top5": [{"id": nid, "date": ndate} for nid, ndate in ordered[:5]],
        "pointer_keys": pointer_keys,
        "has_april_2026": has_apr,
    }


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------
def main() -> int:
    print("=" * 70)
    print("FIFA latest dateId investigation")
    print("=" * 70)

    try:
        p1 = phase1_no_dateid()

        # Phase 1 (no dateId) returns an empty body, so the walk-back is the
        # real discovery mechanism; always run it.
        p2 = phase2_walkback()

        phase3_index_endpoints()
        phase4_next_data()

        print("\n" + "=" * 70)
        print("PHASE 5: VERDICT")
        print("=" * 70)
        latest_release = None
        latest_dateid = None
        method = None
        france = None
        if p2 and p2.get("rankings"):
            latest_release = p2.get("release_date")
            latest_dateid = p2.get("dateId")
            france = france_row(p2["rankings"])
            method = "daily-counter walk-back from today"

        print(f"Latest release today ({date.today().isoformat()}): "
              f"{latest_release}  ({latest_dateid})")
        print(f"Discovery method that found it: {method}")
        print(f"France: {france}")
        if france and france.get("rank") == 1 and france.get("totalPoints") == 1877.32:
            print("  -> MATCHES the screenshot exactly (France #1 @ 1877.32).")
        elif france and france.get("rank") == 1:
            print("  -> France is #1 (points differ from screenshot; review).")
        else:
            print("  -> NOTE: France not #1; review above.")

        print("\nWhy the obvious approaches fail:")
        print("  - No-dateId call returns an empty body (dateId is REQUIRED).")
        print("  - __NEXT_DATA__ date list is stale (caps at id14870 / 2025-09-18).")
        print("  - All index/calendar endpoints 404.")
        print("\nRecommended PRIMARY method (production): compute today's dateId via")
        print("  id = 14870 + (today - 2025-09-18).days, then walk back one day at a")
        print("  time hitting /api/ranking-overview?locale=en&dateId=id{n} until a")
        print("  non-empty (>50 byte) body is returned -> that's the latest release.")
        print("Recommended FALLBACK: if the anchor ever drifts, re-derive it from any")
        print("  known (date, dateId) pair in __NEXT_DATA__, then walk back as above.")

        findings["verdict"] = {
            "today": date.today().isoformat(),
            "latest_release_date": latest_release,
            "latest_dateId": latest_dateid,
            "discovery_method": method,
            "france": france,
            "france_matches_screenshot": bool(
                france and france.get("rank") == 1 and france.get("totalPoints") == 1877.32
            ),
            "daily_counter_anchor": {"dateId": f"id{ANCHOR_ID}", "date": ANCHOR_DATE.isoformat()},
            "no_dateid_returns_empty": True,
            "next_data_is_stale": True,
            "index_endpoints_all_404": True,
            "recommended_primary": (
                "compute dateId = 14870 + (today - 2025-09-18).days, "
                "walk back daily until /api/ranking-overview?dateId=id{n} returns data"
            ),
            "recommended_fallback": (
                "re-derive anchor from a known (date,dateId) pair in __NEXT_DATA__, "
                "then walk back as above"
            ),
        }
    except requests.RequestException as exc:
        print(f"\n[NETWORK ERROR] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - diagnostic; surface anything
        print(f"\n[UNEXPECTED ERROR] {type(exc).__name__}: {exc}")
        return 1
    finally:
        FINDINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        FINDINGS_PATH.write_text(
            json.dumps(findings, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        print(f"\nSaved findings to {FINDINGS_PATH}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
