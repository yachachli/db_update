"""Entry point: python -m nba_game_update"""

import sys
from .pipeline import run_game_update, get_engine


def main():
    try:
        engine = get_engine()
        run_game_update(engine)
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
