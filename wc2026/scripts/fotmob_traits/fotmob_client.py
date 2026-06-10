"""Signed FotMob HTTP client with disk cache and rate limiting."""

from __future__ import annotations

import hashlib
import json
import logging
import time
import urllib.parse
from pathlib import Path
from typing import Any

import requests

from fotmob_auth import build_x_mas_header

logger = logging.getLogger(__name__)

BASE = "https://www.fotmob.com"
SEARCH_PATH = "/api/data/search/suggest"
PLAYER_DATA_PATH = "/api/data/playerData"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.fotmob.com/",
    "Content-Type": "application/json",
}


class FotmobClient:
    def __init__(
        self,
        cache_dir: Path,
        *,
        min_interval_sec: float = 0.25,
        max_retries: int = 4,
    ) -> None:
        self.cache_dir = cache_dir
        self.search_cache = cache_dir / "search"
        self.player_cache = cache_dir / "player"
        self.search_cache.mkdir(parents=True, exist_ok=True)
        self.player_cache.mkdir(parents=True, exist_ok=True)
        self.min_interval_sec = min_interval_sec
        self.max_retries = max_retries
        self._session = requests.Session()
        self._last_request_at = 0.0
        self.network_calls = 0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.min_interval_sec:
            time.sleep(self.min_interval_sec - elapsed)

    def _get_json(self, api_path: str) -> Any:
        headers = {**DEFAULT_HEADERS, "x-mas": build_x_mas_header(api_path)}
        backoff = 1.0
        for attempt in range(self.max_retries):
            self._throttle()
            self._last_request_at = time.monotonic()
            self.network_calls += 1
            resp = self._session.get(BASE + api_path, headers=headers, timeout=30)
            if resp.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries - 1:
                logger.warning("HTTP %s for %s — retry in %.1fs", resp.status_code, api_path, backoff)
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"Failed after retries: {api_path}")

    @staticmethod
    def _search_cache_key(term: str) -> str:
        digest = hashlib.sha256(term.strip().lower().encode("utf-8")).hexdigest()[:16]
        safe = "".join(c if c.isalnum() else "_" for c in term.strip().lower())[:40]
        return f"{safe}_{digest}"

    def search_players(self, term: str) -> list[dict[str, Any]]:
        cache_file = self.search_cache / f"{self._search_cache_key(term)}.json"
        if cache_file.exists():
            data = json.loads(cache_file.read_text(encoding="utf-8"))
        else:
            path = f"{SEARCH_PATH}?term={urllib.parse.quote(term)}"
            data = self._get_json(path)
            cache_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        players: list[dict[str, Any]] = []
        if not isinstance(data, list):
            return players
        seen: set[int] = set()
        for group in data:
            for hit in group.get("suggestions") or []:
                if hit.get("type") != "player":
                    continue
                pid = int(hit["id"])
                if pid in seen:
                    continue
                seen.add(pid)
                players.append(
                    {
                        "id": pid,
                        "name": hit.get("name"),
                        "team": hit.get("teamName"),
                        "team_id": hit.get("teamId"),
                        "score": hit.get("score", 0),
                    }
                )
        players.sort(key=lambda p: p.get("score", 0), reverse=True)
        return players

    def player_data(self, player_id: int) -> dict[str, Any] | None:
        cache_file = self.player_cache / f"{player_id}.json"
        if cache_file.exists():
            data = json.loads(cache_file.read_text(encoding="utf-8"))
        else:
            path = f"{PLAYER_DATA_PATH}?id={player_id}"
            data = self._get_json(path)
            cache_file.write_text(
                json.dumps(data, ensure_ascii=False), encoding="utf-8"
            )
        if not isinstance(data, dict) or not data.get("id"):
            return None
        return data
