"""Smoke test for the SportMonks API connection.

Makes ONE real request (GET /leagues, 5 results) to confirm the API key
works, that authenticated requests succeed, and to reveal the raw response
structure before we write any parsing logic.

Run from the project root:

    python scripts/test_sportmonks_connection.py
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

# Allow running directly (`python scripts/test_sportmonks_connection.py`) by
# putting the project root on sys.path so `import src` resolves.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    print("=" * 70)
    print("SportMonks connection smoke test")
    print("=" * 70)

    try:
        client = SportmonksClient()
    except SportmonksError as exc:
        print(f"\n[CONFIG ERROR] Could not create client: {exc}")
        print("Check that SPORTMONKS_API_KEY is set in your .env file.")
        return 1

    try:
        response = client.get("leagues", params={"per_page": 5})
    except SportmonksError as exc:
        print(f"\n[REQUEST FAILED] {exc}")
        print(
            "This usually means an auth problem (bad/expired key), a network "
            "issue, or an unexpected non-2xx response. See the message above."
        )
        return 1
    except Exception as exc:  # noqa: BLE001 - smoke test wants any failure surfaced
        print(f"\n[UNEXPECTED ERROR] {type(exc).__name__}: {exc}")
        return 1

    print("\n[SUCCESS] Request completed (implies HTTP 2xx).\n")

    print("-" * 70)
    print("FULL RESPONSE STRUCTURE")
    print("-" * 70)
    print(json.dumps(response, indent=2))

    print("\n" + "-" * 70)
    print("SUMMARY")
    print("-" * 70)
    _summarize(response)
    return 0


def _summarize(response: dict) -> None:
    """Print a quick summary of the leagues payload without assuming shape."""
    print(f"Top-level keys: {sorted(response.keys())}")

    data = response.get("data")
    if isinstance(data, list):
        print(f"Number of leagues returned: {len(data)}")
        if data:
            first = data[0]
            if isinstance(first, dict):
                print(f"Fields on each league: {sorted(first.keys())}")
            else:
                print(f"First item is not a dict, it is: {type(first).__name__}")
    else:
        print(
            "No 'data' list found at the top level; inspect the full "
            "structure above to see how leagues are nested."
        )

    if "pagination" in response:
        print(f"Pagination block present: {response['pagination']}")


if __name__ == "__main__":
    raise SystemExit(main())
