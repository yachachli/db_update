"""
Diagnostic: print step-by-step margin calculation for 2–3 games from today's slate.
Shows where our formula might diverge from KenPom (e.g. per-100 vs point margin, HCA).
Run from backend: py -m scripts.diagnose_margin_calculation
"""
from __future__ import annotations

import sys
from pathlib import Path

_backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend_root))

# Same constants as slate_today
HOME_COURT_ADVANTAGE = 5.0
MARGIN_SIGMA = 11.5
NUM_GAMES = 3


def _get_rating(pomeroy, team, col, default):
    from app.services.team_name_resolver import get_rating
    return get_rating(pomeroy, team, col, default)


def main() -> int:
    from app.scrapers.odds_scraper import get_current_odds, parse_game_odds
    from app.services.team_name_resolver import resolve_odds_to_kenpom_verified

    # Load Pomeroy (same as slate_today)
    from app.config import get_cache_dir
    cache_dir = get_cache_dir()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    import pandas as pd
    files = list(cache_dir.glob("pomeroy_ratings_*.parquet"))
    if not files:
        print("KenPom cache not found. Run refresh_kenpom_cache first.", file=sys.stderr)
        return 1
    pomeroy = pd.read_parquet(max(files, key=lambda p: p.stat().st_mtime))

    try:
        games_raw = get_current_odds()
    except Exception as e:
        print(f"Failed to fetch odds: {e}", file=sys.stderr)
        return 1

    count = 0
    for g in games_raw:
        if count >= NUM_GAMES:
            break
        if not isinstance(g, dict):
            continue
        po = parse_game_odds(g)
        if not po or po.get("consensus_spread") is None:
            continue
        home_team = po["home_team"]
        away_team = po["away_team"]
        home_kp = resolve_odds_to_kenpom_verified(home_team, pomeroy)
        away_kp = resolve_odds_to_kenpom_verified(away_team, pomeroy)
        if home_kp is None or away_kp is None:
            continue

        vegas_spread = float(po["consensus_spread"])
        adjO_h = _get_rating(pomeroy, home_kp, "AdjO", 100.0)
        adjD_h = _get_rating(pomeroy, home_kp, "AdjD", 100.0)
        adjT_h = _get_rating(pomeroy, home_kp, "AdjT", 67.0)
        adjO_a = _get_rating(pomeroy, away_kp, "AdjO", 100.0)
        adjD_a = _get_rating(pomeroy, away_kp, "AdjD", 100.0)
        adjT_a = _get_rating(pomeroy, away_kp, "AdjT", 67.0)

        # --- Step-by-step (per 100 possessions) ---
        home_off_vs_away_def = adjO_h - adjD_a
        away_off_vs_home_def = adjO_a - adjD_h
        eff_margin_per_100 = home_off_vs_away_def - away_off_vs_home_def
        eff_margin_with_hca = eff_margin_per_100 + HOME_COURT_ADVANTAGE

        # Our current formula uses eff_margin_with_hca as the POINT margin (no tempo scaling)
        margin_current = eff_margin_with_hca
        tempo = (adjT_h + adjT_a) / 2.0
        # KenPom point margin = efficiency margin (per 100) × (game possessions / 100)
        # Game possessions ≈ tempo (poss per 40 min). So point margin = eff_margin * tempo / 100
        margin_tempo_scaled = eff_margin_with_hca * (tempo / 100.0)

        edge_current = margin_current + vegas_spread
        edge_tempo_scaled = margin_tempo_scaled + vegas_spread

        print()
        print("=" * 70)
        print(f"  {away_kp}  @  {home_kp}")
        print("=" * 70)
        print("  Raw inputs (KenPom cache):")
        print(f"    Home: AdjO={adjO_h:.2f}, AdjD={adjD_h:.2f}, AdjT={adjT_h:.2f}")
        print(f"    Away: AdjO={adjO_a:.2f}, AdjD={adjD_a:.2f}, AdjT={adjT_a:.2f}")
        print()
        print("  Step 1 — Home offense vs away defense (pts/100 poss):")
        print(f"    AdjO_h - AdjD_a = {adjO_h:.2f} - {adjD_a:.2f} = {home_off_vs_away_def:.2f}")
        print("  Step 2 — Away offense vs home defense (pts/100 poss):")
        print(f"    AdjO_a - AdjD_h = {adjO_a:.2f} - {adjD_h:.2f} = {away_off_vs_home_def:.2f}")
        print("  Step 3 — Efficiency margin (per 100 poss), no HCA:")
        print(f"    Step1 - Step2 = {home_off_vs_away_def:.2f} - {away_off_vs_home_def:.2f} = {eff_margin_per_100:.2f}")
        print("  Step 4 — Add home court advantage:")
        print(f"    + HCA ({HOME_COURT_ADVANTAGE}) => {eff_margin_with_hca:.2f}  (still per 100 poss)")
        print()
        print("  Step 5 — Convert to POINT margin (for a ~40-min game):")
        print(f"    Tempo (avg poss/40min) = ({adjT_h:.2f} + {adjT_a:.2f})/2 = {tempo:.2f}")
        print(f"    Point margin = eff_margin_with_hca × (tempo/100) = {eff_margin_with_hca:.2f} × {tempo/100:.3f} = {margin_tempo_scaled:.2f}")
        print()
        print("  Comparison:")
        print(f"    Vegas spread (home POV): {vegas_spread:.2f}")
        print(f"    Our current formula (using per-100 AS point margin): {margin_current:.2f}  =>  edge = {edge_current:.2f}")
        print(f"    Tempo-scaled point margin:                          {margin_tempo_scaled:.2f}  =>  edge = {edge_tempo_scaled:.2f}")
        print()
        print("  Note: If FanMatch PredictedMOV is in POINTS, we should use tempo-scaled margin.")
        print("  Using per-100 as point margin inflates by factor 100/tempo ≈ {:.2f}x.".format(100.0 / tempo if tempo else 0))
        print()

        count += 1

    # Optionally: show a match from ATS (KenPom's PredictedMOV) for same team pair if any
    ats_path = _backend_root / "data" / "historical" / "ats_complete_2026.parquet"
    if ats_path.exists() and count > 0:
        ats = pd.read_parquet(ats_path)
        print("  --- Historical ATS (KenPom PredictedMOV = point margin from FanMatch) ---")
        for col in ["home_team", "away_team", "kenpom_predicted_margin", "vegas_spread", "kenpom_vs_vegas_edge"]:
            if col in ats.columns:
                break
        else:
            col = None
        if col and "kenpom_predicted_margin" in ats.columns:
            sample = ats[ats["kenpom_predicted_margin"].notna()].head(5)
            print("  Sample rows: game_date, home_team, away_team, kenpom_predicted_margin, vegas_spread, kenpom_vs_vegas_edge")
            for _, row in sample.iterrows():
                print(f"    {row.get('game_date')}  {row.get('home_team')}  {row.get('away_team')}  "
                      f"kp_margin={row.get('kenpom_predicted_margin')}  spread={row.get('vegas_spread')}  edge={row.get('kenpom_vs_vegas_edge')}")
            print("  (KenPom PredictedMOV in ATS is in POINTS; our slate_today currently uses per-100 as points.)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
