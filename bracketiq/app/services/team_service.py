"""
Task 6: Team Profile Assembly.
Merge cached KenPom (efficiency, four factors, teamstats, height, pomeroy) into TeamProfile.
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from app.config import get_cache_dir, settings
from app.models.schemas import TeamProfile
from app.services.schedule_service import get_team_schedule as get_schedule_from_fanmatch
from app.services import recency_service


def _latest_parquet(cache_dir: Path, prefix: str) -> Optional[pd.DataFrame]:
    """Load most recent parquet file whose name starts with prefix."""
    files = list(cache_dir.glob(f"{prefix}_*.parquet"))
    if not files:
        return None
    latest = max(files, key=lambda p: p.stat().st_mtime)
    return pd.read_parquet(latest)


def _safe_float(s, default=0.0):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return default
    try:
        return float(s)
    except (TypeError, ValueError):
        return default


def _safe_int(s, default=0):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return default
    try:
        return int(s)
    except (TypeError, ValueError):
        return default


def _normalize_team_name(name: str) -> str:
    """Strip rank/number prefix from KenPom team names for matching."""
    if not name or not isinstance(name, str):
        return ""
    # KenPom: "14 Alabama" or "Duke"
    parts = name.strip().split()
    if parts and parts[0].isdigit():
        return " ".join(parts[1:]).strip()
    return name.strip()


def get_cache_data(season: Optional[str] = None) -> dict[str, Optional[pd.DataFrame]]:
    """Load all cached tables. Keys: efficiency, fourfactors, teamstats_off, teamstats_def, height, pomeroy_ratings."""
    season = season or settings.CURRENT_SEASON or "2026"
    cache_dir = get_cache_dir()
    return {
        "efficiency": _latest_parquet(cache_dir, "efficiency"),
        "fourfactors": _latest_parquet(cache_dir, "fourfactors"),
        "teamstats_off": _latest_parquet(cache_dir, "teamstats_off"),
        "teamstats_def": _latest_parquet(cache_dir, "teamstats_def"),
        "height": _latest_parquet(cache_dir, "height"),
        "pomeroy_ratings": _latest_parquet(cache_dir, "pomeroy_ratings"),
    }


def get_team_profile(team_name: str, season: Optional[str] = None) -> Optional[TeamProfile]:
    """Build a single TeamProfile from cached data. team_name should match KenPom (e.g. 'Duke', 'North Carolina')."""
    data = get_cache_data(season)
    eff = data.get("efficiency")
    ff = data.get("fourfactors")
    tso = data.get("teamstats_off")
    tsd = data.get("teamstats_def")
    height = data.get("height")
    pomeroy = data.get("pomeroy_ratings")

    if eff is None and pomeroy is None:
        return None

    def find_row(df: Optional[pd.DataFrame], name_col: str = "Team") -> Optional[pd.Series]:
        from app.services.team_name_resolver import find_team_row as resolver_find_team_row
        return resolver_find_team_row(df, team_name, name_col=name_col)

    # Prefer pomeroy for rank and AdjO/AdjD/AdjT (same as AdjOE/AdjDE/AdjTempo)
    rank = 0
    adj_oe, adj_de, adj_tempo = 0.0, 0.0, 0.0
    conference = ""

    if pomeroy is not None:
        prow = find_row(pomeroy)
        if prow is not None:
            rank = _safe_int(prow.get("Rk"))
            adj_oe = _safe_float(prow.get("AdjO"))
            adj_de = _safe_float(prow.get("AdjD"))
            adj_tempo = _safe_float(prow.get("AdjT"))
            conference = str(prow.get("Conf", "")) or ""

    if eff is not None and (adj_oe == 0 or adj_de == 0):
        erow = find_row(eff)
        if erow is not None:
            if adj_oe == 0:
                adj_oe = _safe_float(erow.get("Off. Efficiency-Adj"))
            if adj_de == 0:
                adj_de = _safe_float(erow.get("Def. Efficiency-Adj"))
            if adj_tempo == 0:
                adj_tempo = _safe_float(erow.get("Tempo-Adj"))
            if not conference:
                conference = str(erow.get("Conference", "")) or ""

    adj_em = adj_oe - adj_de

    # Four factors (KenPom: percentages as 0-100 or decimal)
    off_efg = off_to = off_or = off_ft_rate = 0.0
    def_efg = def_to = def_or = def_ft_rate = 0.0
    if ff is not None:
        frow = find_row(ff)
        if frow is not None:
            for k, v in frow.items():
                if not isinstance(k, str):
                    continue
                vf = _safe_float(v)
                if vf > 1 and "%" in k:
                    vf = vf / 100.0
                if k == "Off-eFG%":
                    off_efg = vf
                elif k == "Off-TO%":
                    off_to = vf
                elif k == "Off-OR%":
                    off_or = vf
                elif k == "Off-FTRate":
                    off_ft_rate = vf
                elif k == "Def-eFG%":
                    def_efg = vf
                elif k == "Def-TO%":
                    def_to = vf
                elif k == "Def-OR%":
                    def_or = vf
                elif k == "Def-FTRate":
                    def_ft_rate = vf

    # Shooting splits (teamstats offense)
    three_pt_pct = two_pt_pct = ft_pct = three_pt_rate = 0.0
    if tso is not None:
        tsrow = find_row(tso)
        if tsrow is not None:
            three_pt_pct = _safe_float(tsrow.get("3P%"))
            if three_pt_pct > 1:
                three_pt_pct /= 100.0
            two_pt_pct = _safe_float(tsrow.get("2P%"))
            if two_pt_pct > 1:
                two_pt_pct /= 100.0
            ft_pct = _safe_float(tsrow.get("FT%"))
            if ft_pct > 1:
                ft_pct /= 100.0
            three_pt_rate = _safe_float(tsrow.get("3PA%"))
            if three_pt_rate > 1:
                three_pt_rate /= 100.0

    # Height / experience
    avg_height = experience = bench_minutes = 0.0
    if height is not None:
        hrow = find_row(height)
        if hrow is not None:
            avg_height = _safe_float(hrow.get("AvgHgt"))
            experience = _safe_float(hrow.get("Experience"))
            bench_minutes = _safe_float(hrow.get("Bench"))

    display_name = team_name
    if pomeroy is not None:
        prow = find_row(pomeroy)
        if prow is not None:
            display_name = str(prow.get("Team", team_name)).strip()
    elif eff is not None:
        erow = find_row(eff)
        if erow is not None:
            display_name = str(erow.get("Team", team_name)).strip()

    profile = TeamProfile(
        name=display_name,
        conference=conference,
        kenpom_rank=rank,
        adj_oe=adj_oe,
        adj_de=adj_de,
        adj_em=adj_em,
        adj_tempo=adj_tempo,
        off_efg=off_efg,
        off_to=off_to,
        off_or=off_or,
        off_ft_rate=off_ft_rate,
        def_efg=def_efg,
        def_to=def_to,
        def_or=def_or,
        def_ft_rate=def_ft_rate,
        three_pt_pct=three_pt_pct,
        two_pt_pct=two_pt_pct,
        ft_pct=ft_pct,
        three_pt_rate=three_pt_rate,
        avg_height=avg_height,
        experience=experience,
        bench_minutes=bench_minutes,
    )

    # Recency: from reconstructed schedule (Phase 1.5)
    schedule = get_schedule_from_fanmatch(display_name)
    if schedule is not None and len(schedule) > 0 and pomeroy is not None:
        recency = recency_service.calculate_recency_metrics(
            display_name, schedule, pomeroy, window_days=21
        )
        if recency:
            profile.recent_adj_oe = recency["recent_adj_oe"]
            profile.recent_adj_de = recency["recent_adj_de"]
            profile.recent_record = recency["recent_record"]
            profile.recent_ats_record = recency["recent_ats_record"]
            profile.recent_margin_vs_expected = recency["recent_margin_vs_expected"]
            profile.trend_direction = recency["trend_direction"]

    return profile


def list_teams(season: Optional[str] = None) -> list[dict]:
    """List all teams with basic stats (name, rank, conference, adj_em)."""
    data = get_cache_data(season)
    pomeroy = data.get("pomeroy_ratings")
    if pomeroy is None:
        eff = data.get("efficiency")
        if eff is None:
            return []
        teams = []
        for _, row in eff.iterrows():
            name = str(row.get("Team", "")).strip()
            if not name or name == "Team":
                continue
            teams.append({
                "name": _normalize_team_name(name) or name,
                "kenpom_rank": 0,
                "conference": str(row.get("Conference", "")),
                "adj_em": _safe_float(row.get("Off. Efficiency-Adj", 0)) - _safe_float(row.get("Def. Efficiency-Adj", 0)),
            })
        return teams
    teams = []
    for _, row in pomeroy.iterrows():
        name = str(row.get("Team", "")).strip()
        if not name or name == "Team":
            continue
        teams.append({
            "name": _normalize_team_name(name) or name,
            "kenpom_rank": _safe_int(row.get("Rk")),
            "conference": str(row.get("Conf", "")),
            "adj_em": _safe_float(row.get("AdjEM")),
        })
    return teams


def get_team_schedule_cached(team_name: str, season: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Return reconstructed schedule from FanMatch data (schedule_service)."""
    return get_schedule_from_fanmatch(team_name)
