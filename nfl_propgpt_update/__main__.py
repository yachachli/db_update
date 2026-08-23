"""Map db_update Neon secrets onto ``scripts/weekly_run.py``.

Expected checkout layout::

    db_update/                  # this repo
    nfl-propgpt-model/          # zernerdoescode/nfl-propgpt-model

Env (any one of the DB forms is enough):

    TANK01_API_KEY or RAPIDAPI_KEY
    DB_URL or DATABASE_URL or DB_USER + DB_PASSWORD + DB_HOST + DB_NAME
    DB_SCHEMA   (default nfl_model_v2)
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def neon_url() -> str:
    if os.environ.get("DB_URL"):
        return os.environ["DB_URL"]
    if os.environ.get("DATABASE_URL"):
        return os.environ["DATABASE_URL"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    host = os.environ["DB_HOST"]
    name = os.environ["DB_NAME"]
    return f"postgresql://{user}:{password}@{host}/{name}?sslmode=require"


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not os.environ.get("TANK01_API_KEY"):
        os.environ["TANK01_API_KEY"] = os.environ.get("RAPIDAPI_KEY") or ""
    os.environ["DB_URL"] = neon_url()
    os.environ.setdefault("DB_SCHEMA", "nfl_model_v2")
    root = Path(os.environ.get("NFL_MODEL_ROOT", "nfl-propgpt-model")).resolve()
    script = root / "scripts" / "weekly_run.py"
    if not script.is_file():
        raise SystemExit(f"nfl-propgpt-model not checked out at {root}")
    return subprocess.call([sys.executable, str(script), *args], cwd=root)


if __name__ == "__main__":
    raise SystemExit(main())
