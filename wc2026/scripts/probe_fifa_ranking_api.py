"""Diagnostic: find and probe FIFA's underlying JSON ranking API.

The ranking page hydrates its table client-side (confirmed in
probe_fifa_rankings_page.py), so the data must come from an API call. This
script extracts hints from the page's ``__NEXT_DATA__`` blob (available ranking
date IDs, any API paths) and probes the likely ranking endpoint, verifying the
response actually contains team names and point values.

Separate data source from SportMonks: uses plain ``requests``.

Run from the project root:

    python scripts/probe_fifa_ranking_api.py
    python scripts/probe_fifa_ranking_api.py --force   # re-fetch the HTML
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_HTML_PATH = _PROJECT_ROOT / "data" / "fifa_rankings_raw.html"
API_RESPONSE_PATH = _PROJECT_ROOT / "data" / "fifa_ranking_api_response.json"

PAGE_URL = "https://inside.fifa.com/fifa-world-ranking/men"
# Known FIFA ranking API shape on inside.fifa.com (Next.js backend).
API_BASE = "https://inside.fifa.com/api/ranking-overview"
REQUEST_TIMEOUT_SECONDS = 30
CACHE_MAX_AGE_SECONDS = 24 * 60 * 60

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": PAGE_URL,
}


# ---------------------------------------------------------------------------
# Phase 1: get the page HTML and extract __NEXT_DATA__
# ---------------------------------------------------------------------------
def get_html(force: bool) -> str:
    fresh = (
        RAW_HTML_PATH.exists()
        and (time.time() - RAW_HTML_PATH.stat().st_mtime) < CACHE_MAX_AGE_SECONDS
    )
    if not force and fresh:
        print(f"Using cached HTML: {RAW_HTML_PATH}")
        return RAW_HTML_PATH.read_text(encoding="utf-8", errors="replace")
    print(f"Fetching page {PAGE_URL} ...")
    resp = requests.get(
        PAGE_URL,
        headers={**BROWSER_HEADERS, "Accept": "text/html,*/*"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    RAW_HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
    RAW_HTML_PATH.write_text(resp.text, encoding="utf-8")
    return resp.text


def extract_next_data(html: str) -> dict[str, Any] | None:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not match:
        print("Could not locate __NEXT_DATA__ script tag.")
        return None
    try:
        return json.loads(match.group(1))
    except ValueError as exc:
        print(f"__NEXT_DATA__ was not valid JSON: {exc}")
        return None


def find_date_ids(obj: Any) -> list[dict[str, str]]:
    """Recursively collect {id, date} entries that look like ranking dates."""
    found: list[dict[str, str]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            node_id = node.get("id")
            node_date = node.get("date")
            if (
                isinstance(node_id, str)
                and node_id.startswith("id")
                and isinstance(node_date, str)
                and re.match(r"\d{4}-\d{2}-\d{2}", node_date)
            ):
                found.append({"id": node_id, "date": node_date})
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(obj)
    # Deduplicate, then sort by date descending.
    unique = {entry["id"]: entry for entry in found}
    return sorted(unique.values(), key=lambda e: e["date"], reverse=True)


# ---------------------------------------------------------------------------
# Phase 2: probe the API
# ---------------------------------------------------------------------------
def probe_api(date_id: str | None) -> tuple[int, Any]:
    params = {"locale": "en"}
    if date_id:
        params["dateId"] = date_id
    print(f"\nProbing {API_BASE} with params={params} ...")
    resp = requests.get(
        API_BASE, headers=BROWSER_HEADERS, params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    print(f"  HTTP {resp.status_code} | {len(resp.content):,} bytes | "
          f"content-type={resp.headers.get('content-type')}")
    if resp.status_code != 200:
        return resp.status_code, None
    try:
        return 200, resp.json()
    except ValueError:
        print("  Response was not JSON.")
        return 200, None


def analyze_ranking_payload(payload: Any) -> bool:
    """Check the API payload actually carries teams + point values."""
    point_like = re.findall(r"\b1?\d{3}\.\d{1,2}\b", json.dumps(payload))
    has_points = len(point_like) > 0

    rankings = _find_ranking_list(payload)
    print(f"  point-like values in payload: {len(point_like)} "
          f"(sample: {sorted(set(point_like), reverse=True)[:5]})")
    if rankings:
        print(f"  ranking list found: {len(rankings)} entries; sample top 5:")
        for entry in rankings[:5]:
            print(f"    {_describe_entry(entry)}")
    else:
        print("  No obvious ranking list array located in payload.")
    return has_points and bool(rankings)


def _find_ranking_list(obj: Any) -> list[dict[str, Any]] | None:
    """Find the largest list of dicts that look like ranking rows."""
    best: list[dict[str, Any]] | None = None

    def walk(node: Any) -> None:
        nonlocal best
        if isinstance(node, list) and node and all(isinstance(i, dict) for i in node):
            keys = set().union(*(i.keys() for i in node))
            if {"rankingItem"} & keys or {"totalPoints", "name"} & keys or "totalPoints" in keys:
                if best is None or len(node) > len(best):
                    best = node
        if isinstance(node, dict):
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(obj)
    return best


def _describe_entry(entry: dict[str, Any]) -> str:
    rank = entry.get("rankingItem", {})
    if isinstance(rank, dict):
        name = rank.get("name") or entry.get("name")
    else:
        name = entry.get("name")
    return (
        f"rank={entry.get('rank') or entry.get('rankingItem', {})}  "
        f"name={name}  totalPoints={entry.get('totalPoints')}  "
        f"keys={sorted(entry.keys())[:8]}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe FIFA ranking JSON API.")
    parser.add_argument("--force", action="store_true", help="Re-fetch the page HTML.")
    args = parser.parse_args()

    print("=" * 70)
    print("FIFA ranking API discovery probe")
    print("=" * 70)

    try:
        html = get_html(force=args.force)
        next_data = extract_next_data(html)
        date_ids = find_date_ids(next_data) if next_data else []

        print("\n" + "=" * 70)
        print("PHASE 1: RANKING DATE IDS FROM __NEXT_DATA__")
        print("=" * 70)
        print(f"Found {len(date_ids)} ranking date IDs.")
        for entry in date_ids[:5]:
            print(f"  {entry['id']}  ({entry['date']})")
        latest_id = date_ids[0]["id"] if date_ids else None

        print("\n" + "=" * 70)
        print("PHASE 2: PROBE THE API")
        print("=" * 70)
        working: tuple[str | None, Any] | None = None
        for candidate in [latest_id, None]:
            status, payload = probe_api(candidate)
            if status == 200 and payload is not None:
                if analyze_ranking_payload(payload):
                    working = (candidate, payload)
                    break

        print("\n" + "=" * 70)
        print("VERDICT")
        print("=" * 70)
        if working:
            date_id, payload = working
            API_RESPONSE_PATH.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            url = f"{API_BASE}?locale=en" + (f"&dateId={date_id}" if date_id else "")
            print("VERDICT: API endpoint works and returns ranking data.")
            print(f"  Endpoint: {url}")
            print(f"  Saved sample response to {API_RESPONSE_PATH}")
        else:
            print("VERDICT: Could not confirm a working ranking API endpoint.")
            print("  Next: inspect the page's Network tab for the exact XHR URL,")
            print("  or try a headless browser to capture the request.")
            return 1
    except requests.RequestException as exc:
        print(f"\n[NETWORK ERROR] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - diagnostic; surface anything
        print(f"\n[UNEXPECTED ERROR] {type(exc).__name__}: {exc}")
        return 1

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
