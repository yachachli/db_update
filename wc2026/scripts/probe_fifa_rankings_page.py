"""Diagnostic: is the FIFA men's ranking data present in the raw HTML?

Before committing to a parsing approach, this fetches the FIFA world ranking
page and checks whether team names and point values are server-rendered into
the HTML (vs hydrated client-side). It does NOT parse the rankings -- it only
determines feasibility.

Separate data source from SportMonks: uses plain ``requests``.

Run from the project root:

    python scripts/probe_fifa_rankings_page.py
    python scripts/probe_fifa_rankings_page.py --force   # bypass the cache
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import requests

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_HTML_PATH = _PROJECT_ROOT / "data" / "fifa_rankings_raw.html"

FIFA_URL = "https://inside.fifa.com/fifa-world-ranking/men"
REQUEST_TIMEOUT_SECONDS = 30
CACHE_MAX_AGE_SECONDS = 24 * 60 * 60  # 1 day

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Markers to test for in the raw HTML.
TEAM_NAME_MARKERS = ["France", "Argentina", "Brazil", "Spain", "England"]
POINT_VALUE_MARKERS = ["1877.32"]  # France's total from the reference screenshot
STRUCTURAL_MARKERS = ["points-cell", "world-ranking"]


def _cache_is_fresh() -> bool:
    if not RAW_HTML_PATH.exists():
        return False
    age = time.time() - RAW_HTML_PATH.stat().st_mtime
    return age < CACHE_MAX_AGE_SECONDS


def fetch_html(force: bool) -> str:
    """Return the page HTML, from the 1-day cache unless stale or forced."""
    if not force and _cache_is_fresh():
        age_hours = (time.time() - RAW_HTML_PATH.stat().st_mtime) / 3600
        print(f"Using cached HTML ({age_hours:.1f}h old): {RAW_HTML_PATH}")
        return RAW_HTML_PATH.read_text(encoding="utf-8", errors="replace")

    print(f"Fetching {FIFA_URL} ...")
    response = requests.get(
        FIFA_URL, headers=BROWSER_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS
    )
    print(f"HTTP status: {response.status_code}")
    print(f"Response size: {len(response.content):,} bytes")

    if response.status_code in (403, 429):
        raise _BlockedError(response.status_code)
    response.raise_for_status()

    RAW_HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
    RAW_HTML_PATH.write_text(response.text, encoding="utf-8")
    print(f"Saved raw HTML to {RAW_HTML_PATH}")
    return response.text


class _BlockedError(RuntimeError):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"Blocked with HTTP {status_code}")
        self.status_code = status_code


def test_markers(html: str) -> dict[str, bool]:
    """Print PRESENT/NOT FOUND for each marker group; return presence flags."""
    print("\n" + "=" * 70)
    print("MARKER PRESENCE")
    print("=" * 70)

    def check(label: str, markers: list[str]) -> bool:
        any_present = False
        print(f"\n{label}:")
        for marker in markers:
            count = html.count(marker)
            if count:
                any_present = True
                print(f"  PRESENT ({count:>4}x)  {marker!r}")
            else:
                print(f"  NOT FOUND        {marker!r}")
        return any_present

    return {
        "team_names": check("Team names", TEAM_NAME_MARKERS),
        "point_values": check("Point values", POINT_VALUE_MARKERS),
        "structural": check("Structural markers", STRUCTURAL_MARKERS),
    }


def print_verdict(flags: dict[str, bool]) -> None:
    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)
    names = flags["team_names"]
    points = flags["point_values"]

    if names and points:
        print("VERDICT: Server-rendered. Standard HTML parsing will work.")
    elif names and not points:
        print(
            "VERDICT: Partial render. Names static, points may be hydrated "
            "client-side. Investigate further."
        )
    else:
        print(
            "VERDICT: Fully client-rendered. Will need headless browser "
            "(Playwright/Selenium) or a different data source."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe FIFA ranking page HTML.")
    parser.add_argument(
        "--force", action="store_true", help="Bypass the 1-day HTML cache."
    )
    args = parser.parse_args()

    print("=" * 70)
    print("FIFA men's world ranking page probe")
    print("=" * 70)

    try:
        html = fetch_html(force=args.force)
    except _BlockedError as exc:
        print(f"\n[BLOCKED] {exc}.")
        print("Mitigations to try:")
        print("  - Rotate to a more recent/real browser User-Agent string.")
        print("  - Add a delay between requests (time.sleep) and retry.")
        print("  - Send a Referer header and cookies from a real session.")
        print("  - Use a different network/IP, or a headless browser instead.")
        return 1
    except requests.Timeout:
        print("\n[TIMEOUT] Request exceeded 30s. Retry or increase the timeout.")
        return 1
    except requests.RequestException as exc:
        print(f"\n[NETWORK ERROR] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - diagnostic; surface anything
        print(f"\n[UNEXPECTED ERROR] {type(exc).__name__}: {exc}")
        return 1

    flags = test_markers(html)
    print_verdict(flags)
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
