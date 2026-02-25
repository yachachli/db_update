"""Entry point: python -m nba_odds_update"""

import sys
from .pipeline import run_odds_update
from .db import get_engine


def main():
    try:
        engine = get_engine()
        run_odds_update(engine)
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
