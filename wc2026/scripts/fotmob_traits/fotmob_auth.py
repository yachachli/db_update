"""FotMob x-mas request signing (live-verified helper for Phase 0+)."""

from __future__ import annotations

import base64
import hashlib
import json
import time
from pathlib import Path

# Secret lyrics for MD5 signature — copied verbatim from FotMob client (Three Lions).
# Stored as a file so empty lines are preserved exactly.
_SECRET_PATH = Path(__file__).with_name("fotmob_secret_lyrics.txt")


def _load_secret_lyrics() -> str:
    return _SECRET_PATH.read_text(encoding="utf-8")


def build_x_mas_header(api_path: str, *, timestamp_ms: int | None = None) -> str:
    """Return the base64 ``x-mas`` header for a FotMob API path including query string.

    ``api_path`` must start with ``/api/...`` e.g. ``/api/playerData?id=15580``.
    """
    if not api_path.startswith("/api/"):
        raise ValueError(f"api_path must start with /api/, got {api_path!r}")

    code = int(time.time() * 1000) if timestamp_ms is None else timestamp_ms
    body = {"url": api_path, "code": code}
    payload = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
    combined = payload + _load_secret_lyrics()
    signature = hashlib.md5(combined.encode("utf-8")).hexdigest().upper()
    token = json.dumps({"body": body, "signature": signature}, separators=(",", ":"))
    return base64.b64encode(token.encode("utf-8")).decode("ascii")
