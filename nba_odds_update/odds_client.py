"""Minimal Odds API v4 client — live/upcoming NBA odds."""

import time
import requests

TEAM_NAME_TO_ABV = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "LA Clippers": "LAC",
    "LA Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}


class OddsClient:
    BASE_URL = "https://api.the-odds-api.com/v4"
    SPORT = "basketball_nba"

    def __init__(self, api_key: str, rate_limit: float = 1.0):
        self.api_key = api_key
        self.rate_limit = rate_limit
        self.last_request_time = 0

    def _wait(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request_time = time.time()

    def get_live_odds(self) -> list[dict]:
        """Fetch current/upcoming NBA odds (spreads, totals, h2h)."""
        self._wait()
        url = f"{self.BASE_URL}/sports/{self.SPORT}/odds"
        try:
            resp = requests.get(url, params={
                "apiKey": self.api_key,
                "regions": "us",
                "markets": "spreads,totals,h2h",
                "oddsFormat": "american",
            }, timeout=30)
            if resp.status_code in (401, 422):
                print(f"  Odds API returned {resp.status_code}")
                return []
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except requests.RequestException as exc:
            print(f"  Odds API error: {exc}")
            return []
