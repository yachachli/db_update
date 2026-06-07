"""Confirm SportMonks has the WC 2026 finals fixtures populated.

A ~30-second diagnostic, run before committing to a fixture-driven
architecture. Queries the World Cup finals league (732) for fixtures in the
next 60 days, summarizes what comes back, and checks whether anything falls in
the scheduled WC 2026 window (11 Jun - 19 Jul 2026).

Read-only and cache-friendly. Saves the raw response to
data/wc2026_fixtures_check.json.

Run from the project root:

    python scripts/verify_wc2026_fixtures_available.py
    python scripts/verify_wc2026_fixtures_available.py --force-refresh
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.sportmonks_client import (  # noqa: E402
    WC_FINALS_LEAGUE_ID,
    SportmonksClient,
    SportmonksError,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

_LOOKAHEAD_DAYS = 60
_OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "wc2026_fixtures_check.json"

# WC 2026 is scheduled 11 June - 19 July 2026.
_WC_WINDOW_START = date(2026, 6, 11)
_WC_WINDOW_END = date(2026, 7, 19)


def _fixture_date(fixture: dict[str, Any]) -> str:
    """Best-effort 'YYYY-MM-DD' for a fixture, from starting_at."""
    raw = fixture.get("starting_at")
    if isinstance(raw, str) and len(raw) >= 10:
        return raw[:10]
    return ""


def _participant_names(fixture: dict[str, Any]) -> str:
    """Render 'Team A vs Team B' from the included participants, if present."""
    participants = fixture.get("participants")
    if isinstance(participants, list) and participants:
        names = [p.get("name", "?") for p in participants]
        return " vs ".join(names)
    return fixture.get("name", "?")


def _venue_name(fixture: dict[str, Any]) -> str:
    venue = fixture.get("venue")
    if isinstance(venue, dict):
        return venue.get("name", "?")
    return "?"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force-refresh", action="store_true",
                        help="Bypass the on-disk cache and re-fetch.")
    args = parser.parse_args()

    today = date.today()
    end = today + timedelta(days=_LOOKAHEAD_DAYS)

    print("=" * 78)
    print("WC 2026 FIXTURE AVAILABILITY CHECK")
    print("=" * 78)
    print(f"Querying finals league {WC_FINALS_LEAGUE_ID} for fixtures "
          f"{today.isoformat()} -> {end.isoformat()} ...")

    try:
        client = SportmonksClient()
        response = client.get(
            f"fixtures/between/{today.isoformat()}/{end.isoformat()}",
            params={
                "filters": f"fixtureLeagues:{WC_FINALS_LEAGUE_ID}",
                "include": "participants;state;venue",
                "per_page": 100,
            },
            force_refresh=args.force_refresh,
        )
    except SportmonksError as exc:
        print(f"\nERROR: SportMonks request failed: {exc}")
        return 1

    # Persist the raw response for later inspection.
    _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(
        json.dumps(response, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Saved raw response to {_OUTPUT_PATH}")

    fixtures = response.get("data")
    if not isinstance(fixtures, list):
        fixtures = []

    rate = response.get("rate_limit")
    if rate:
        print(f"Rate limit: {rate}")

    # -- Summary ---------------------------------------------------------
    print("\n" + "-" * 78)
    print(f"Total fixtures returned: {len(fixtures)}")

    dated = [d for d in (_fixture_date(f) for f in fixtures) if d]
    if dated:
        print(f"Date range: {min(dated)} -> {max(dated)}")
        by_month = Counter(d[:7] for d in dated)
        print("Count by month:")
        for month in sorted(by_month):
            print(f"  {month}: {by_month[month]}")
    else:
        print("Date range: (no dated fixtures returned)")

    print("\nSample fixtures (up to 10):")
    if fixtures:
        for f in fixtures[:10]:
            print(f"  id={f.get('id')}  {_fixture_date(f) or '????-??-??'}  "
                  f"{_participant_names(f)}  @ {_venue_name(f)}")
    else:
        print("  (none)")

    # -- Cross-check the WC 2026 window ----------------------------------
    in_window = [
        f for f in fixtures
        if _fixture_date(f)
        and _WC_WINDOW_START.isoformat() <= _fixture_date(f) <= _WC_WINDOW_END.isoformat()
    ]
    print("\n" + "-" * 78)
    print(f"WC 2026 window ({_WC_WINDOW_START} to {_WC_WINDOW_END}): "
          f"{len(in_window)} fixtures")
    if not in_window:
        print("WARN: No fixtures fall inside the scheduled WC 2026 window. "
              "The finals draw/schedule may not be loaded on our plan yet, or "
              "today's date precedes the window's appearance in the feed.")

    # -- Verdict ---------------------------------------------------------
    print("\n" + "=" * 78)
    print("VERDICT")
    print("=" * 78)
    if in_window:
        print(f"GOOD: SportMonks has {len(in_window)} fixtures in the WC 2026 "
              "window -> proceed with a fixture-driven architecture.")
        return 0
    print("BAD: SportMonks does not have WC 2026 fixtures populated -> we need "
          "a fallback manual import (e.g. the official schedule once the draw "
          "is finalized).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
