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

    def get_scores_for_date(self, date: str, max_retries: int = 3) -> list[dict]:
        """Fetch scores for all NBA games on *date* (YYYYMMDD)."""
        url = f"{self.BASE_URL}/getNBAScoresOnly"
        
        for attempt in range(max_retries):
            self._wait()
            try:
                resp = requests.get(
                    url, headers=self.headers,
                    params={"gameDate": date}, timeout=30,
                )
                
                # Handle rate limiting
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    wait_time = float(retry_after) if retry_after else (2 ** attempt) * 2
                    if attempt < max_retries - 1:
                        print(f"  Rate limited for {date}, waiting {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"  Tank01 error for {date}: 429 Too Many Requests (exhausted retries)")
                        return []
                
                # Handle forbidden (might be API key issue or date not available)
                if resp.status_code == 403:
                    print(f"  Tank01 error for {date}: 403 Forbidden (API key issue or date not available)")
                    return []
                
                resp.raise_for_status()
                data = resp.json()
                break
                
            except requests.RequestException as exc:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 1
                    print(f"  Error for {date}: {exc}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  Tank01 error for {date}: {exc} (exhausted retries)")
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
