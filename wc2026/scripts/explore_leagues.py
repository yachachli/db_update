"""One-shot exploration of the full league universe on our SportMonks plan.

Diagnostic, NOT production code. Its only job is to reveal what leagues exist
and how SportMonks categorizes them (``type`` / ``sub_type`` / ``category``)
so we can design the competition classifier in a later step. No classification
or business logic is performed here.

Run from the project root (cached after the first run):

    python scripts/explore_leagues.py
"""

from __future__ import annotations

import json
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

# Allow direct execution by putting the project root on sys.path.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402

logger = logging.getLogger("explore_leagues")

PER_PAGE = 50
CATALOG_PATH = _PROJECT_ROOT / "data" / "leagues_catalog.json"

# Rate-limit guard rails.
RATE_LIMIT_WARN = 100
RATE_LIMIT_ABORT = 10

# Name substrings we care about for the model (label -> list of substrings).
LOOKUPS: dict[str, list[str]] = {
    "World Cup": ["world cup"],
    "Nations League": ["nations league"],
    "Euro": ["euro"],
    "Copa America": ["copa america"],
    "AFCON / Africa Cup": ["afcon", "africa cup"],
    "Asian Cup": ["asian cup"],
    "Gold Cup": ["gold cup"],
    "Friendlies": ["friendly", "friendlies"],
}


class RateLimitAbort(RuntimeError):
    """Raised to stop pagination when remaining quota is dangerously low."""


def fetch_all_leagues(client: SportmonksClient) -> list[dict[str, Any]]:
    """Paginate through GET /leagues and collect every league.

    Stops when the API reports no further pages, or aborts early if the
    remaining rate-limit quota drops below ``RATE_LIMIT_ABORT``.
    """
    leagues: list[dict[str, Any]] = []
    page = 1

    while True:
        response = client.get(
            "leagues", params={"per_page": PER_PAGE, "page": page}
        )

        page_data = response.get("data", [])
        if isinstance(page_data, list):
            leagues.extend(page_data)

        _check_rate_limit(response.get("rate_limit"))

        pagination = response.get("pagination") or {}
        logger.info(
            "Fetched page %s (%d leagues this page, %d total so far)",
            pagination.get("current_page", page),
            len(page_data),
            len(leagues),
        )

        if not pagination.get("has_more"):
            break
        page += 1

    return leagues


def _check_rate_limit(rate_limit: dict[str, Any] | None) -> None:
    """Warn when remaining quota is low; abort when it is nearly exhausted."""
    if not isinstance(rate_limit, dict):
        return
    remaining = rate_limit.get("remaining")
    if not isinstance(remaining, int):
        return
    if remaining < RATE_LIMIT_ABORT:
        raise RateLimitAbort(
            f"Rate limit nearly exhausted ({remaining} remaining); aborting "
            f"to avoid a hard block. Resets in "
            f"{rate_limit.get('resets_in_seconds')}s."
        )
    if remaining < RATE_LIMIT_WARN:
        logger.warning(
            "Rate limit getting low: %d requests remaining (resets in %ss).",
            remaining,
            rate_limit.get("resets_in_seconds"),
        )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def print_field_summary(leagues: list[dict[str, Any]]) -> None:
    """For type / sub_type / category, show value counts and examples."""
    print("\n" + "=" * 70)
    print("FIELD SUMMARY")
    print("=" * 70)
    print(f"Total leagues: {len(leagues)}")

    for field in ("type", "sub_type", "category"):
        counts: Counter[Any] = Counter(league.get(field) for league in leagues)
        print(f"\n--- {field} ({len(counts)} unique values) ---")
        for value, count in counts.most_common():
            examples = [
                league.get("name", "<no name>")
                for league in leagues
                if league.get(field) == value
            ][:3]
            print(f"  {value!r}: {count} leagues | e.g. {examples}")


def print_grouped_by_subtype(leagues: list[dict[str, Any]]) -> None:
    """Dump every league grouped by sub_type, sorted by name within group."""
    print("\n" + "=" * 70)
    print("LEAGUES GROUPED BY sub_type")
    print("=" * 70)

    groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for league in leagues:
        groups[league.get("sub_type")].append(league)

    for sub_type in sorted(groups, key=lambda v: str(v)):
        group = sorted(groups[sub_type], key=lambda lg: str(lg.get("name", "")))
        print(f"\n--- sub_type = {sub_type!r} ({len(group)} leagues) ---")
        for lg in group:
            print(
                f"  id={lg.get('id')} | {lg.get('name')!r} | "
                f"type={lg.get('type')} | sub_type={lg.get('sub_type')} | "
                f"category={lg.get('category')} | "
                f"country_id={lg.get('country_id')} | active={lg.get('active')}"
            )


def print_targeted_lookups(leagues: list[dict[str, Any]]) -> None:
    """Print leagues whose name matches the competitions we care about."""
    print("\n" + "=" * 70)
    print("TARGETED LOOKUPS (competitions relevant to the model)")
    print("=" * 70)

    for label, needles in LOOKUPS.items():
        matches = [
            lg
            for lg in leagues
            if any(n in str(lg.get("name", "")).lower() for n in needles)
        ]
        print(f"\n--- {label} ({len(matches)} match(es)) ---")
        if not matches:
            print("  (none found)")
            continue
        for lg in sorted(matches, key=lambda lg: str(lg.get("name", ""))):
            print(
                f"  id={lg.get('id')} | {lg.get('name')!r} | "
                f"type={lg.get('type')} | sub_type={lg.get('sub_type')} | "
                f"category={lg.get('category')} | "
                f"last_played_at={lg.get('last_played_at')}"
            )


def save_catalog(leagues: list[dict[str, Any]]) -> None:
    """Persist the full league list for reference by later steps."""
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CATALOG_PATH.write_text(
        json.dumps(leagues, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\nSaved {len(leagues)} leagues to {CATALOG_PATH}")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    print("=" * 70)
    print("SportMonks league universe exploration")
    print("=" * 70)

    try:
        client = SportmonksClient()
    except SportmonksError as exc:
        print(f"\n[CONFIG ERROR] {exc}")
        return 1

    try:
        leagues = fetch_all_leagues(client)
    except RateLimitAbort as exc:
        print(f"\n[ABORTED] {exc}")
        return 1
    except SportmonksError as exc:
        print(f"\n[REQUEST FAILED] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - diagnostic script, surface anything
        print(f"\n[UNEXPECTED ERROR] {type(exc).__name__}: {exc}")
        return 1

    if not leagues:
        print("\n[WARNING] No leagues returned; nothing to explore.")
        return 1

    print_field_summary(leagues)
    print_grouped_by_subtype(leagues)
    print_targeted_lookups(leagues)
    save_catalog(leagues)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
