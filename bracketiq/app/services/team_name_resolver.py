"""
BracketIQ — Central team name resolution for Odds API ↔ KenPom.
Single source of truth so slate_today, model_analysis, build_ats_dataset, and team_service
all resolve and lookup names consistently. Prevents mismatches (e.g. Tenn-Martin vs Tennessee Martin).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# KenPom cache may use "Team" (kenpompy) or "team"; we try both.
TEAM_COLUMN_CANDIDATES = ("Team", "team")


def _normalize_for_match(name: str) -> str:
    """Strip rank prefix and normalize for comparison."""
    if not name or not isinstance(name, str):
        return ""
    s = str(name).strip()
    parts = s.split()
    if parts and parts[0].isdigit():
        s = " ".join(parts[1:]).strip()
    return s.lower()


def _get_aliases_path() -> Path:
    base = Path(__file__).resolve().parent.parent.parent
    for candidate in (base / "data", base.parent / "backend" / "data"):
        p = candidate / "kenpom_aliases.json"
        if p.exists():
            return p
    return base / "data" / "kenpom_aliases.json"


def _load_kenpom_aliases() -> dict[str, list[str]]:
    """Load canonical KenPom name -> list of strings that may appear in cache."""
    path = _get_aliases_path()
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("kenpom_aliases", data) if isinstance(data, dict) else {}
    except Exception:
        return {}


# Lazy-loaded alias map: canonical -> [alias1, alias2, ...]
_kenpom_aliases: Optional[dict[str, list[str]]] = None


def get_kenpom_aliases() -> dict[str, list[str]]:
    global _kenpom_aliases
    if _kenpom_aliases is None:
        _kenpom_aliases = _load_kenpom_aliases()
    return _kenpom_aliases


def _alias_to_canonical() -> dict[str, str]:
    """Map any alias (or canonical) -> canonical KenPom name."""
    m: dict[str, str] = {}
    for canonical, aliases in get_kenpom_aliases().items():
        c = canonical.strip()
        if c:
            m[c.lower()] = c
        for a in aliases:
            if a and a.strip():
                m[a.strip().lower()] = c if c else canonical
    return m


def resolve_to_canonical_kenpom(any_name: str) -> str:
    """
    Given any team name (Odds API or KenPom variant like Tenn-Martin), return canonical KenPom name.
    Use for consistent matching in build_ats_dataset and elsewhere.
    """
    s = (any_name or "").strip()
    if not s:
        return s
    alias_map = _alias_to_canonical()
    if s.lower() in alias_map:
        return alias_map[s.lower()]
    from app.scrapers.odds_scraper import odds_to_kenpom_name
    from_odds = (odds_to_kenpom_name(s) or s).strip()
    if from_odds.lower() in alias_map:
        return alias_map[from_odds.lower()]
    return from_odds


def fanmatch_match_key(name: str) -> str:
    """
    Single normalization key for FanMatch ↔ slate matching.
    Use this (or normalize after resolve_to_canonical_kenpom) so both sides use the same key.
    Handles: St./State, A&M/AM, trailing conference names, case, punctuation.
    """
    if not name or not isinstance(name, str):
        return ""
    s = name.strip().lower().rstrip(".")
    while "  " in s:
        s = s.replace("  ", " ")
    s = re.sub(
        r"\s+(big east|big ten|big 12|acc|sec|aac|wcc|mwc|pac-?12|ivy|mvc|atlantic 10|big west|maac|horizon|summit|wac|conference usa|c-usa)$",
        "",
        s,
        flags=re.IGNORECASE,
    ).strip()
    if s.endswith(" st"):
        s = s[:-3] + " state"
    s = s.replace("&", "")
    return s


def resolve_odds_to_kenpom(odds_name: str) -> str:
    """
    Resolve Odds API team name to canonical KenPom name.
    Uses odds_scraper.odds_to_kenpom_name so all mapping lives in one place.
    """
    from app.scrapers.odds_scraper import odds_to_kenpom_name
    return (odds_to_kenpom_name(odds_name) or odds_name).strip()


def resolve_odds_to_kenpom_verified(
    odds_name: str,
    pomeroy_df: Optional[pd.DataFrame] = None,
) -> Optional[str]:
    """
    Resolve Odds API name to KenPom name and verify it exists in the Pomeroy cache.
    Returns None if unresolved or not found in cache (caller should skip the game).
    """
    from app.scrapers.odds_scraper import odds_to_kenpom_name
    resolved = (odds_to_kenpom_name(odds_name or "") or "").strip()
    if not resolved:
        return None
    if pomeroy_df is None or pomeroy_df.empty:
        return resolved
    row = find_team_row(pomeroy_df, resolved)
    return resolved if row is not None else None


def get_names_to_try_for_lookup(canonical_kenpom: str) -> list[str]:
    """
    Return list of strings to try when searching a KenPom dataframe (e.g. pomeroy).
    First element should be the canonical name, then aliases (e.g. Tennessee Martin, UT Martin, Tenn-Martin).
    """
    aliases_map = get_kenpom_aliases()
    names = [canonical_kenpom.strip()]
    if canonical_kenpom in aliases_map:
        for a in aliases_map[canonical_kenpom]:
            if a and a not in names:
                names.append(a)
    return names


def find_team_row(
    df: pd.DataFrame | None,
    kenpom_name: str,
    name_col: Optional[str] = None,
) -> pd.Series | None:
    """
    Find a row in a KenPom-derived dataframe (e.g. pomeroy_ratings) by team name.
    Uses canonical name and kenpom_aliases so that "Tennessee Martin" matches
    cache rows that say "Tennessee Martin", "UT Martin", or "Tenn-Martin".
    - Exact match (after stripping rank prefix) is preferred.
    - Then tries all aliases for kenpom_name.
    - No ambiguous single-word or substring matching (avoids "Martin" matching wrong team).
    """
    if df is None or df.empty:
        return None
    # Resolve to canonical if we have an odds-style name
    canonical = (kenpom_name or "").strip()
    names_to_try = get_names_to_try_for_lookup(canonical)
    # Determine name column
    if name_col is not None:
        if name_col not in df.columns:
            return None
        cols = [name_col]
    else:
        cols = [c for c in TEAM_COLUMN_CANDIDATES if c in df.columns]
        if not cols:
            return None
    for _, row in df.iterrows():
        for col in cols:
            val = row.get(col)
            if val is None:
                continue
            nstr = _normalize_for_match(str(val))
            if not nstr:
                continue
            for candidate in names_to_try:
                if not candidate:
                    continue
                if nstr == _normalize_for_match(candidate):
                    return row
    return None


# Column aliases: KenPom cache may use AdjT; Neon/parquet may use adj_tempo. Try all so we always get tempo.
_RATING_COL_ALIASES: dict[str, list[str]] = {
    "AdjT": ["AdjT", "adj_tempo", "AdjTempo", "adj_t"],
    "AdjO": ["AdjO", "adj_oe", "AdjOE"],
    "AdjD": ["AdjD", "adj_de", "AdjDE"],
}


def get_rating(
    df: pd.DataFrame | None,
    kenpom_name: str,
    col: str,
    default: Optional[float] = 100.0,
) -> Optional[float]:
    """Get a numeric rating (e.g. AdjO, AdjD, AdjT) for a team from a KenPom dataframe.
    Tries column aliases (AdjT/adj_tempo etc.) so we get the value whether source is parquet or Neon.
    If default is None and the value is missing, returns None (caller can flag/skip)."""
    row = find_team_row(df, kenpom_name)
    if row is None:
        return default
    aliases = _RATING_COL_ALIASES.get(col, [])
    candidates = [col] + [a for a in aliases if a != col]
    for c in candidates:
        if c not in row.index and c not in row:
            continue
        v = row.get(c)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return default
