"""
Task 3: Odds Data Pipeline — The Odds API.
Current/historical odds, consensus lines, implied probability, team name mapper.
"""

import json
import os
from pathlib import Path
from typing import Optional

import httpx

from app.config import settings


ODDS_API_BASE = "https://api.the-odds-api.com/v4"
REGIONS = "us"
MARKETS = "h2h,spreads,totals"
ODDS_FORMAT = "american"


# Team name normalization: KenPom (short) <-> The Odds API (full names)
# Odds API often uses "Team Mascot" e.g. "Duke Blue Devils"
TEAM_NAME_MAPPER: dict[str, str] = {
    "Duke": "Duke Blue Devils",
    "North Carolina": "North Carolina Tar Heels",
    "Kentucky": "Kentucky Wildcats",
    "Kansas": "Kansas Jayhawks",
    "UConn": "Connecticut Huskies",
    "Connecticut": "Connecticut Huskies",
    "Virginia": "Virginia Cavaliers",
    "Gonzaga": "Gonzaga Bulldogs",
    "Villanova": "Villanova Wildcats",
    "Michigan State": "Michigan State Spartans",
    "Michigan": "Michigan Wolverines",
    "Ohio State": "Ohio State Buckeyes",
    "Purdue": "Purdue Boilermakers",
    "Indiana": "Indiana Hoosiers",
    "Wisconsin": "Wisconsin Badgers",
    "Illinois": "Illinois Fighting Illini",
    "Iowa": "Iowa Hawkeyes",
    "Maryland": "Maryland Terrapins",
    "Rutgers": "Rutgers Scarlet Knights",
    "Penn State": "Penn State Nittany Lions",
    "Nebraska": "Nebraska Cornhuskers",
    "Northwestern": "Northwestern Wildcats",
    "Minnesota": "Minnesota Golden Gophers",
    "Alabama": "Alabama Crimson Tide",
    "Auburn": "Auburn Tigers",
    "Tennessee": "Tennessee Volunteers",
    "Arkansas": "Arkansas Razorbacks",
    "Florida": "Florida Gators",
    "Georgia": "Georgia Bulldogs",
    "LSU": "LSU Tigers",
    "Ole Miss": "Ole Miss Rebels",
    "Mississippi State": "Mississippi State Bulldogs",
    "South Carolina": "South Carolina Gamecocks",
    "Texas A&M": "Texas A&M Aggies",
    "Vanderbilt": "Vanderbilt Commodores",
    "Missouri": "Missouri Tigers",
    "Arizona": "Arizona Wildcats",
    "UCLA": "UCLA Bruins",
    "USC": "USC Trojans",
    "Oregon": "Oregon Ducks",
    "Washington": "Washington Huskies",
    "Colorado": "Colorado Buffaloes",
    "Utah": "Utah Utes",
    "Stanford": "Stanford Cardinal",
    "California": "California Golden Bears",
    "Washington State": "Washington State Cougars",
    "Oregon State": "Oregon State Beavers",
    "Arizona State": "Arizona State Sun Devils",
    "Baylor": "Baylor Bears",
    "Texas": "Texas Longhorns",
    "Kansas": "Kansas Jayhawks",
    "Oklahoma": "Oklahoma Sooners",
    "Oklahoma State": "Oklahoma State Cowboys",
    "Iowa State": "Iowa State Cyclones",
    "West Virginia": "West Virginia Mountaineers",
    "TCU": "TCU Horned Frogs",
    "Texas Tech": "Texas Tech Red Raiders",
    "Kansas State": "Kansas State Wildcats",
    "Wake Forest": "Wake Forest Demon Deacons",
    "NC State": "NC State Wolfpack",
    "Syracuse": "Syracuse Orange",
    "Louisville": "Louisville Cardinals",
    "Clemson": "Clemson Tigers",
    "Florida State": "Florida State Seminoles",
    "Miami (FL)": "Miami Hurricanes",
    "Miami": "Miami Hurricanes",
    "Virginia Tech": "Virginia Tech Hokies",
    "Notre Dame": "Notre Dame Fighting Irish",
    "Pittsburgh": "Pittsburgh Panthers",
    "Boston College": "Boston College Eagles",
    "Georgia Tech": "Georgia Tech Yellow Jackets",
    "Marquette": "Marquette Golden Eagles",
    "Creighton": "Creighton Bluejays",
    "Xavier": "Xavier Musketeers",
    "Providence": "Providence Friars",
    "Seton Hall": "Seton Hall Pirates",
    "St. John's": "St. John's Red Storm",
    "Butler": "Butler Bulldogs",
    "DePaul": "DePaul Blue Demons",
    "Georgetown": "Georgetown Hoyas",
}
# Schools where "St" is part of the name (do not strip to avoid "Kansas St" -> "Kansas")
ST_SCHOOLS: dict[str, str] = {
    "Michigan St Spartans": "Michigan St.",
    "Michigan St.": "Michigan St.",
    "Kansas St Wildcats": "Kansas St.",
    "Kansas St.": "Kansas St.",
    "Florida St Seminoles": "Florida St.",
    "Florida St.": "Florida St.",
    "Arizona St Sun Devils": "Arizona St.",
    "Arizona St.": "Arizona St.",
    "Oregon St Beavers": "Oregon St.",
    "Oregon St.": "Oregon St.",
    "Oklahoma St Cowboys": "Oklahoma St.",
    "Oklahoma St.": "Oklahoma St.",
    "San Diego St Aztecs": "San Diego St.",
    "San Diego St.": "San Diego St.",
    "Mississippi St Bulldogs": "Mississippi St.",
    "Mississippi St.": "Mississippi St.",
    "Illinois St Redbirds": "Illinois St.",
    "Illinois St.": "Illinois St.",
    "Washington St Cougars": "Washington St.",
    "Washington St.": "Washington St.",
    "Colorado St Rams": "Colorado St.",
    "Colorado St.": "Colorado St.",
    "Jacksonville St Gamecocks": "Jacksonville St.",
    "Indiana St Sycamores": "Indiana St.",
    "Indiana St.": "Indiana St.",
    "Georgia St Panthers": "Georgia St.",
    "Georgia St.": "Georgia St.",
    "Missouri St Bears": "Missouri St.",
    "Missouri St.": "Missouri St.",
    "North Dakota St Bison": "North Dakota St.",
    "South Dakota St Jackrabbits": "South Dakota St.",
    "Central Connecticut St Blue Devils": "Central Connecticut",
    "Northwestern St Demons": "Northwestern St.",
    "Montana St Bobcats": "Montana St.",
    "Alabama St Hornets": "Alabama St.",
    "Nicholls St Colonels": "Nicholls",
    "South Carolina St Bulldogs": "South Carolina St.",
    "Tennessee St Tigers": "Tennessee St.",
    "Portland St Vikings": "Portland St.",
    "Portland St.": "Portland St.",
    "Delaware St Hornets": "Delaware St.",
    "New Mexico St Aggies": "New Mexico St.",
    "New Mexico St.": "New Mexico St.",
    "Utah St Aggies": "Utah St.",
    "Utah St.": "Utah St.",
    "Iowa St Cyclones": "Iowa St.",
    "Iowa St.": "Iowa St.",
    "Penn St Nittany Lions": "Penn St.",
    "Penn St.": "Penn St.",
    "Ohio St Buckeyes": "Ohio St.",
    "Ohio St.": "Ohio St.",
    "Boise St Broncos": "Boise St.",
    "Boise St.": "Boise St.",
    "Fresno St Bulldogs": "Fresno St.",
    "Sam Houston St Bearkats": "Sam Houston St.",
    "Wichita St Shockers": "Wichita St.",
    "Wichita St.": "Wichita St.",
    "Kennesaw St Owls": "Kennesaw St.",
    "Morehead St Eagles": "Morehead St.",
    "Norfolk St Spartans": "Norfolk St.",
    "Alcorn St Braves": "Alcorn St.",
    "Grambling St Tigers": "Grambling St.",
    "Coppin St Eagles": "Coppin St.",
    "Murray St Racers": "Murray St.",
    "Murray St.": "Murray St.",
    "Weber St Wildcats": "Weber St.",
    "Idaho St Bengals": "Idaho St.",
    "Sacramento St Hornets": "Sacramento St.",
    "Cleveland St Vikings": "Cleveland St.",
    "Youngstown St Penguins": "Youngstown St.",
    "Wright St Raiders": "Wright St.",
    "McNeese St Cowboys": "McNeese",
    "Appalachian St Mountaineers": "Appalachian St.",
    "Ball St Cardinals": "Ball St.",
    "Bowie St Bulldogs": "Bowie St.",
    # Odds API uses full mascot names; avoid "X St Red/Golden/etc" stripping errors
    "Arkansas St Red Wolves": "Arkansas St.",
    "Arkansas St.": "Arkansas St.",
    "Kent State Golden Flashes": "Kent St.",
    "Kent St.": "Kent St.",
    "Hawai'i Rainbow Warriors": "Hawaii",
    "Hawai'i": "Hawaii",
    "Mississippi Valley St Delta Devils": "Mississippi Valley St.",
    "Miss Valley St Delta Devils": "Mississippi Valley St.",
    "Mississippi Valley St.": "Mississippi Valley St.",
    "Arkansas-Pine Bluff Golden Lions": "Ark.-Pine Bluff",
    "Ark.-Pine Bluff": "Ark.-Pine Bluff",
    "CSU Bakersfield Roadrunners": "Cal St. Bakersfield",
    "Cal St. Bakersfield": "Cal St. Bakersfield",
    "SE Missouri St Redhawks": "Southeast Missouri St.",
    "Southeast Missouri St.": "Southeast Missouri St.",
    "UIC Flames": "UIC",
    "Illinois Chicago Flames": "UIC",
    "Miami (OH) RedHawks": "Miami (OH)",
    "Ohio Bobcats": "Ohio",
}
# Mascot adjectives that appear before final mascot noun; strip so "Arkansas St Red" -> "Arkansas St."
_MASCOT_ADJECTIVES = frozenset({"Red", "Golden", "Rainbow", "Delta", "Fighting", "Flying", "Raging", "Screaming", "Purple"})
# Built-in reverse map (never modified); used by build_team_name_mapping.py
BUILTIN_ODDS_TO_KENPOM = {v: k for k, v in TEAM_NAME_MAPPER.items()}
# Runtime map: Odds API -> KenPom (merged with team_name_mapping.json if present)
ODDS_TO_KENPOM = dict(BUILTIN_ODDS_TO_KENPOM)


def _load_team_name_mapping() -> None:
    """Merge team_name_mapping.json into ODDS_TO_KENPOM and TEAM_NAME_MAPPER."""
    for base in (Path(__file__).resolve().parent.parent.parent, Path(__file__).resolve().parent.parent):
        path = base / "data" / "team_name_mapping.json"
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                odds_to_k = data.get("odds_to_kenpom", {})
                ODDS_TO_KENPOM.update(odds_to_k)
                for odds_name, kenpom_name in odds_to_k.items():
                    if kenpom_name not in TEAM_NAME_MAPPER:
                        TEAM_NAME_MAPPER[kenpom_name] = odds_name
            except Exception:
                pass
            break


_load_team_name_mapping()


def _api_key() -> str:
    key = getattr(settings, "ODDS_API_KEY", None) or os.environ.get("ODDS_API_KEY", "")
    if not key:
        raise ValueError("ODDS_API_KEY not set")
    return key


def get_current_odds() -> list[dict]:
    """Current NCAAB odds: h2h, spreads, totals. Returns list of game objects."""
    url = f"{ODDS_API_BASE}/sports/basketball_ncaab/odds"
    params = {
        "regions": REGIONS,
        "markets": MARKETS,
        "oddsFormat": ODDS_FORMAT,
        "apiKey": _api_key(),
    }
    with httpx.Client() as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        return r.json()


def get_historical_odds(date: str) -> list[dict]:
    """Historical odds for backtesting (paid plan). date = YYYY-MM-DD or full ISO8601 (e.g. 2025-11-04T12:00:00Z)."""
    # API requires ISO8601 timestamp; if given YYYY-MM-DD only, use noon UTC
    if len(date) == 10 and date[4] == "-" and date[7] == "-":
        date = f"{date}T12:00:00Z"
    url = f"{ODDS_API_BASE}/historical/sports/basketball_ncaab/odds"
    params = {
        "regions": REGIONS,
        "markets": MARKETS,
        "oddsFormat": ODDS_FORMAT,
        "date": date,
        "apiKey": _api_key(),
    }
    with httpx.Client() as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        return r.json()


def get_scores() -> list[dict]:
    """Completed game scores."""
    url = f"{ODDS_API_BASE}/sports/basketball_ncaab/scores"
    params = {"apiKey": _api_key()}
    with httpx.Client() as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        return r.json()


def calculate_consensus_line(bookmakers: list[dict], market: str = "spreads") -> Optional[dict]:
    """Average spread/total across books. Returns e.g. { 'spread': -8.0, 'total': 147.5 }.
    Note: For spread, use calculate_consensus_line_for_game(game) so only the HOME team's
    outcome is used (otherwise home -7.5 and away +7.5 average to 0)."""
    spreads = []
    totals = []
    for b in bookmakers:
        for m in b.get("markets", []):
            if m.get("key") == "spreads" and market == "spreads":
                for o in m.get("outcomes", []):
                    if "point" in o:
                        spreads.append(o["point"])
            if m.get("key") == "totals":
                for o in m.get("outcomes", []):
                    if o.get("name") == "Over" and "point" in o:
                        totals.append(o["point"])
    out = {}
    if spreads:
        out["spread"] = sum(spreads) / len(spreads)
    if totals:
        out["total"] = sum(totals) / len(totals)
    return out if out else None


def _outcome_matches_home(outcome_name: str, home_team: str) -> bool:
    """True if this outcome is for the home team (exact or fuzzy substring only)."""
    if not outcome_name or not home_team:
        return False
    on = str(outcome_name).strip()
    ht = str(home_team).strip()
    if on == ht:
        return True
    on_l, ht_l = on.lower(), ht.lower()
    if on_l == ht_l:
        return True
    # Fuzzy: one contains the other (e.g. "Duke" vs "Duke Blue Devils", "Texas" vs "Texas Longhorns")
    # Avoid matching "Texas" to "Texas A&M" by requiring word boundary: shorter is prefix of longer
    if ht_l in on_l or on_l in ht_l:
        return True
    return False


def parse_game_odds(game: dict) -> Optional[dict]:
    """Extract consensus spread from the Odds API HOME team's perspective only.
    Uses exact then fuzzy name match so we never take the away team's point.
    Returns consensus_spread (negative = home favored), consensus_total, num_bookmakers."""
    home_team = game.get("home_team", "")
    away_team = game.get("away_team", "")
    home_spreads = []
    totals = []
    for bookmaker in game.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            if market.get("key") == "spreads":
                home_point = None
                outcomes = market.get("outcomes", [])
                for outcome in outcomes:
                    if _outcome_matches_home(outcome.get("name", ""), home_team) and "point" in outcome:
                        home_point = float(outcome["point"])
                        break
                if home_point is not None:
                    home_spreads.append(home_point)
            elif market.get("key") == "totals":
                for outcome in market.get("outcomes", []):
                    if outcome.get("name") == "Over" and "point" in outcome:
                        totals.append(float(outcome["point"]))
                        break
    consensus_spread = sum(home_spreads) / len(home_spreads) if home_spreads else None
    consensus_total = sum(totals) / len(totals) if totals else None
    if consensus_spread is None and consensus_total is None:
        return None
    return {
        "home_team": home_team,
        "away_team": away_team,
        "consensus_spread": consensus_spread,
        "consensus_total": consensus_total,
        "num_bookmakers": len(home_spreads) or len(totals),
    }


def calculate_implied_probability(american_odds: int) -> float:
    """Convert American odds to implied win probability (0-1)."""
    if american_odds > 0:
        return 100.0 / (american_odds + 100)
    return abs(american_odds) / (abs(american_odds) + 100)


def get_team_futures() -> list[dict]:
    """Tournament/championship futures if available. Many NCAAB futures are seasonal."""
    url = f"{ODDS_API_BASE}/sports/basketball_ncaab/odds"
    params = {
        "regions": REGIONS,
        "markets": "outrights",
        "oddsFormat": ODDS_FORMAT,
        "apiKey": _api_key(),
    }
    try:
        with httpx.Client() as client:
            r = client.get(url, params=params)
            if r.status_code != 200:
                return []
            return r.json()
    except Exception:
        return []


def kenpom_to_odds_name(kenpom_name: str) -> str:
    """Map KenPom team name to Odds API name."""
    return TEAM_NAME_MAPPER.get(kenpom_name.strip(), kenpom_name.strip())


def odds_to_kenpom_name(odds_name: str) -> str:
    """Map Odds API team name to KenPom name. Explicit mapping first; fallback strips mascot(s) and normalizes St."""
    s = odds_name.strip()
    if not s:
        return s
    if s in ST_SCHOOLS:
        return ST_SCHOOLS[s]
    if s in ODDS_TO_KENPOM:
        return ODDS_TO_KENPOM[s]
    parts = s.split()
    if len(parts) < 2:
        return s
    base = " ".join(parts[:-1])
    # If base ends with mascot adjective (e.g. "Arkansas St Red"), strip it so we get "Arkansas St."
    if len(parts) >= 3 and parts[-2] in _MASCOT_ADJECTIVES:
        base = " ".join(parts[:-2])
    # Normalize " St" -> " St." for KenPom (e.g. "Arkansas St" -> "Arkansas St.")
    if base.endswith(" St") and not base.endswith(" St."):
        base = base + "."
    return base
