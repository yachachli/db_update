"""Cron job: fetch the latest FIFA rankings into Neon.

Run from the project root:

    python scripts/cron_fetch_fifa.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import DatabaseError, upsert_fifa_ranking  # noqa: E402
from src.fifa_rankings import FifaRankingsClient, FifaRankingsError  # noqa: E402


def main() -> int:
    print("=" * 78)
    print("CRON: FETCH FIFA RANKINGS")
    print("=" * 78)

    try:
        release = FifaRankingsClient().fetch_latest()
    except FifaRankingsError as exc:
        print(f"ERROR: FIFA fetch failed: {exc}")
        return 1

    release_date = date.fromisoformat(release.release_date)
    count = 0

    try:
        for entry in release.entries:
            upsert_fifa_ranking(entry, release_date)
            count += 1
    except DatabaseError as exc:
        print(f"ERROR: database upsert failed: {exc}")
        return 1

    print(f"\nStored {count} rankings for release date {release_date.isoformat()}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
