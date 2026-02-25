"""Entry point: python -m nba_ratings_update"""

import sys
from .pipeline import run_ratings_update
from .db import get_engine


def main():
    try:
        engine = get_engine()
        run_ratings_update(engine)
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
