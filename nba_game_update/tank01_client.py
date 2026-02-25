"""Minimal Tank01 API client — game scores only."""

import time
import requests


class Tank01Client:
    BASE_URL = "https://tank01-fantasy-stats.p.rapidapi.com"

    def __init__(self, api_key: str, rate_limit: float = 0.5):
        self.api_key = api_key
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "tank01-fantasy-stats.p.rapidapi.com",
        }

    def _wait(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request_time = time.time()

    def get_scores_for_date(self, date: str) -> list[dict]:
        """Fetch scores for all NBA games on *date* (YYYYMMDD)."""
        self._wait()
        url = f"{self.BASE_URL}/getNBAScoresOnly"
        try:
            resp = requests.get(
                url, headers=self.headers,
                params={"gameDate": date}, timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            print(f"  Tank01 error for {date}: {exc}")
            return []

        body = data.get("body")
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            games = []
            for gid, gdata in body.items():
                if isinstance(gdata, dict):
                    gdata.setdefault("gameDate", date)
                    games.append(gdata)
            return games
        return []
