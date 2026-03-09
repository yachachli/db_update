"""
KenPom data pipeline using kenpompy.
https://github.com/j-andrews7/kenpompy

Rate-limited (8s + jitter between requests). All tables cached as parquet in data/cache/.
"""

import time
import random
from pathlib import Path
from datetime import datetime
from typing import Any, Callable, Optional

import pandas as pd

from bs4 import BeautifulSoup

from kenpompy.utils import login, get_html
import kenpompy.summary as kp_summary
import kenpompy.misc as kp_misc
from kenpompy.team import get_schedule
from kenpompy.FanMatch import FanMatch

from app.config import get_cache_dir, get_historical_dir, settings


# Minimum delay between KenPom requests (seconds). Add jitter to avoid robotic pattern.
RATE_LIMIT_SEC = 8
JITTER_SEC = (1, 3)

# URL used to verify we're logged in (summary page; has data tables when access is OK)
_VERIFY_URL = "https://kenpom.com/summary.php?y=2026"


def _delay() -> None:
    """Apply rate limit + jitter."""
    time.sleep(RATE_LIMIT_SEC + random.uniform(*JITTER_SEC))


def get_kenpom_browser():
    """Return an authenticated CloudScraper for KenPom. Uses KENPOM_EMAIL and KENPOM_PASSWORD from config."""
    if not settings.KENPOM_EMAIL or not settings.KENPOM_PASSWORD:
        raise ValueError("Set KENPOM_EMAIL and KENPOM_PASSWORD in .env")
    return login(settings.KENPOM_EMAIL, settings.KENPOM_PASSWORD)


def verify_kenpom_login(browser) -> None:
    """
    Fetch a KenPom data page and check that it contains tables.
    If not (e.g. login wall or block in CI), raise a clear error so you know to check credentials/secrets.
    """
    html = get_html(browser, _VERIFY_URL)
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError(
            "KenPom returned a page with no data tables—login or access likely failed (common in CI). "
            "Check that KENPOM_EMAIL and KENPOM_PASSWORD secrets are set correctly and that your KenPom account is valid."
        )


def get_cached_or_scrape(
    table_name: str,
    scrape_fn: Callable[..., pd.DataFrame],
    cache_dir: Optional[Path] = None,
    max_age_hours: Optional[float] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Load table from cache if present and fresh; otherwise scrape and cache.
    Cache key is table_name + optional kwargs (e.g. season, defense) in the filename.
    """
    cache_dir = cache_dir or get_cache_dir()
    safe_key = "_".join(f"{k}={v}" for k, v in sorted(kwargs.items()) if v is not None)
    base = f"{table_name}_{safe_key}" if safe_key else table_name
    pattern = f"{base}_*.parquet"
    existing = list(cache_dir.glob(pattern))

    if existing:
        path = max(existing, key=lambda p: p.stat().st_mtime)
        if max_age_hours is not None:
            age_hours = (time.time() - path.stat().st_mtime) / 3600
            if age_hours <= max_age_hours:
                return pd.read_parquet(path)
        else:
            return pd.read_parquet(path)

    df = scrape_fn(**kwargs)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = cache_dir / f"{base}_{timestamp}.parquet"
    df.to_parquet(out_path, index=False)
    return df


def _season() -> str:
    return settings.CURRENT_SEASON or "2026"


def refresh_all(browser) -> None:
    """
    Scrape all KenPom tables and cache as parquet. Uses 8s + jitter between calls.
    """
    season = _season()
    cache_dir = get_cache_dir()

    # (table_name, scrape_fn, kwargs_for_filename, call_args)
    # call_args: (browser,) + tuple passed to fn after browser
    tables = [
        ("efficiency", kp_summary.get_efficiency, {"season": season}, (season,)),
        ("fourfactors", kp_summary.get_fourfactors, {"season": season}, (season,)),
        ("teamstats_off", lambda b, s: kp_summary.get_teamstats(b, defense=False, season=s), {"season": season}, (season,)),
        ("teamstats_def", lambda b, s: kp_summary.get_teamstats(b, defense=True, season=s), {"season": season}, (season,)),
        ("height", kp_summary.get_height, {"season": season}, (season,)),
        ("pointdist", kp_summary.get_pointdist, {"season": season}, (season,)),
        ("kpoy", lambda b, s: _kpoy_list_to_df(kp_summary.get_kpoy(b, s)), {"season": season}, (season,)),
        ("pomeroy_ratings", kp_misc.get_pomeroy_ratings, {"season": season}, (season,)),
        ("trends", kp_misc.get_trends, {}, ()),
        ("program_ratings", kp_misc.get_program_ratings, {}, ()),
        ("arenas", kp_misc.get_arenas, {"season": season}, (season,)),
        ("hca", kp_misc.get_hca, {}, ()),
    ]

    for name, fn, kw, args in tables:
        _delay()
        try:
            df = fn(browser, *args)
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            key = "_".join(f"{k}={v}" for k, v in sorted(kw.items()) if v is not None)
            out = cache_dir / f"{name}_{key}_{ts}.parquet"
            df.to_parquet(out, index=False)
        except Exception as e:
            raise RuntimeError(f"refresh_all failed for {name}: {e}") from e

    # Player stats: multiple metrics (some metrics may fail due to kenpompy/table layout)
    for metric in ("ORtg", "Min", "eFG", "OR", "TO", "Stl", "TS", "2P", "3P", "FT"):
        _delay()
        try:
            df = kp_summary.get_playerstats(browser, season=season, metric=metric)
            cache_dir = get_cache_dir()
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            out = cache_dir / f"playerstats_{metric}_{season}_{ts}.parquet"
            if isinstance(df, list):
                pd.concat(df, ignore_index=True).to_parquet(out, index=False)
            else:
                df.to_parquet(out, index=False)
        except Exception as e:
            # Skip metrics that fail (e.g. column count mismatch in kenpompy)
            pass

    # Game attributes: one metric (Excitement) to keep refresh smaller; others can be added
    _delay()
    try:
        df = kp_misc.get_gameattribs(browser, season=season, metric="Excitement")
        cache_dir = get_cache_dir()
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out = cache_dir / f"gameattribs_Excitement_{season}_{ts}.parquet"
        df.to_parquet(out, index=False)
    except Exception as e:
        raise RuntimeError(f"refresh_all failed for gameattribs: {e}") from e


def _kpoy_list_to_df(kpoy_dfs: list) -> pd.DataFrame:
    """Concatenate KPOY list of dataframes into one."""
    return pd.concat(kpoy_dfs, ignore_index=True)


def get_fanmatch_for_date(browser, date: str) -> Optional[FanMatch]:
    """Fetch FanMatch for a given date (YYYY-MM-DD). Applies rate limit before call."""
    _delay()
    return FanMatch(browser, date=date)


def get_team_schedule(browser, team: str, season: Optional[str] = None) -> pd.DataFrame:
    """Team schedule for recency weighting. Rate limit before calling from bulk jobs."""
    _delay()
    return get_schedule(browser, team=team, season=season or _season())
