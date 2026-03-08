"""
Build the ATS dataset by joining Odds API data (source of truth for
home/away and spreads) with FanMatch data (source of truth for
KenPom predictions and actual results). No parsing of FanMatch Game column for home/away.
"""
import sys
from pathlib import Path

import pandas as pd

_backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend_root))

from app.config import get_historical_dir
from app.services.team_name_resolver import resolve_to_canonical_kenpom


def _same_team(name_a: str, name_b: str) -> bool:
    """True if two team names refer to the same team (via canonical KenPom name)."""
    if pd.isna(name_a) or pd.isna(name_b):
        return False
    can_a = resolve_to_canonical_kenpom(str(name_a).strip())
    can_b = resolve_to_canonical_kenpom(str(name_b).strip())
    return (can_a and can_b and can_a.lower() == can_b.lower())


def main():
    hist_dir = get_historical_dir()
    if not hist_dir.is_absolute():
        hist_dir = Path.cwd() / hist_dir
    odds_path = hist_dir / "odds_2026.parquet"
    fm_path = hist_dir / "fanmatch_2026.parquet"
    if not odds_path.exists() or not fm_path.exists():
        for candidate in ["data/historical", "app/data/historical"]:
            cand = (_backend_root / candidate).resolve()
            if (cand / "odds_2026.parquet").exists() and (cand / "fanmatch_2026.parquet").exists():
                hist_dir = cand
                odds_path = cand / "odds_2026.parquet"
                fm_path = cand / "fanmatch_2026.parquet"
                break
    if not odds_path.exists():
        print("odds_2026.parquet not found. Run collect_historical_odds first.")
        return 1
    if not fm_path.exists():
        print("fanmatch_2026.parquet not found.")
        return 1

    odds_df = pd.read_parquet(odds_path)
    fm_df = pd.read_parquet(fm_path)
    rows = []
    matched = 0
    unmatched = 0

    for _, odds_row in odds_df.iterrows():
        date_val = odds_row.get("game_date")
        if pd.isna(date_val):
            continue
        date_str = str(date_val)[:10]
        odds_home = odds_row.get("home_team_kenpom") or odds_row.get("home_team", "")
        odds_away = odds_row.get("away_team_kenpom") or odds_row.get("away_team", "")
        spread = odds_row.get("consensus_spread")
        total = odds_row.get("consensus_total")
        if pd.isna(spread):
            continue
        spread = float(spread)

        fm_on_date = fm_df[fm_df["fanmatch_date"].astype(str).str[:10] == date_str]
        fm_match = None
        for _, fm_row in fm_on_date.iterrows():
            winner = fm_row.get("Winner", "")
            loser = fm_row.get("Loser", "")
            if pd.isna(winner) or pd.isna(loser):
                continue
            winner_str = str(winner).strip()
            loser_str = str(loser).strip()
            teams_match = (
                (_same_team(odds_home, winner_str) and _same_team(odds_away, loser_str))
                or (_same_team(odds_home, loser_str) and _same_team(odds_away, winner_str))
            )
            if teams_match:
                fm_match = fm_row
                break

        if fm_match is None:
            unmatched += 1
            continue
        matched += 1

        winner = fm_match.get("Winner")
        actual_mov = fm_match.get("ActualMOV")
        if pd.isna(actual_mov):
            unmatched += 1
            matched -= 1
            continue
        actual_mov = float(actual_mov)
        winner_str = str(winner).strip()

        if _same_team(winner_str, odds_home):
            actual_margin_home = actual_mov
        elif _same_team(winner_str, odds_away):
            actual_margin_home = -actual_mov
        else:
            unmatched += 1
            matched -= 1
            continue

        predicted_winner = fm_match.get("PredictedWinner", "")
        predicted_mov = fm_match.get("PredictedMOV")
        if predicted_mov is not None and not pd.isna(predicted_mov):
            predicted_mov = float(predicted_mov)
            pred_str = str(predicted_winner).strip()
            if _same_team(pred_str, odds_home):
                kenpom_predicted_margin = predicted_mov
            elif _same_team(pred_str, odds_away):
                kenpom_predicted_margin = -predicted_mov
            else:
                kenpom_predicted_margin = None
        else:
            kenpom_predicted_margin = None

        winner_score = fm_match.get("WinnerScore")
        loser_score = fm_match.get("LoserScore")
        try:
            actual_total = float(winner_score) + float(loser_score) if winner_score is not None and loser_score is not None else None
        except (TypeError, ValueError):
            actual_total = None

        covered_vegas = (actual_margin_home + spread) > 0
        covered_kenpom = (actual_margin_home >= kenpom_predicted_margin - 0.5) if kenpom_predicted_margin is not None else None
        kenpom_vs_vegas_edge = (kenpom_predicted_margin + spread) if kenpom_predicted_margin is not None else None

        if actual_total is not None and total is not None and not pd.isna(total):
            t = float(total)
            if actual_total > t + 0.5:
                over_under = "over"
            elif actual_total < t - 0.5:
                over_under = "under"
            else:
                over_under = "push"
        else:
            over_under = None

        winner_rank = fm_match.get("WinnerRank")
        loser_rank = fm_match.get("LoserRank")
        try:
            if _same_team(winner_str, odds_home):
                home_rank = int(winner_rank) if winner_rank is not None and not pd.isna(winner_rank) else None
                away_rank = int(loser_rank) if loser_rank is not None and not pd.isna(loser_rank) else None
            else:
                home_rank = int(loser_rank) if loser_rank is not None and not pd.isna(loser_rank) else None
                away_rank = int(winner_rank) if winner_rank is not None and not pd.isna(winner_rank) else None
        except (TypeError, ValueError):
            home_rank = None
            away_rank = None

        rows.append({
            "game_date": date_str,
            "home_team": odds_home,
            "away_team": odds_away,
            "home_rank": home_rank,
            "away_rank": away_rank,
            "kenpom_predicted_margin": kenpom_predicted_margin,
            "vegas_spread": spread,
            "actual_margin_home": actual_margin_home,
            "covered_vegas": covered_vegas,
            "covered_kenpom": covered_kenpom,
            "kenpom_vs_vegas_edge": kenpom_vs_vegas_edge,
            "vegas_total": float(total) if total is not None and not pd.isna(total) else None,
            "actual_total": actual_total,
            "over_under_result": over_under,
        })

    out_path = hist_dir / "ats_complete_2026.parquet"
    if not rows:
        print("No matched rows.")
        return 0
    df = pd.DataFrame(rows)
    df.to_parquet(out_path, index=False)

    home_win_rate = (df["actual_margin_home"] > 0).mean()
    cov_vegas = df["covered_vegas"].mean()
    cov_kp = df["covered_kenpom"].dropna().mean() if df["covered_kenpom"].notna().any() else None
    kp_correct = ((df["kenpom_predicted_margin"] > 0) == (df["actual_margin_home"] > 0)).mean()
    kp_correct = kp_correct * 100 if df["kenpom_predicted_margin"].notna().any() else None

    print(f"Matched: {matched}, Unmatched: {unmatched}")
    print(f"\nHome win rate: {home_win_rate:.1%} (expect ~58-62%)")
    print(f"covered_vegas rate: {cov_vegas:.1%} (expect ~47-53%)")
    print(f"covered_kenpom rate: {cov_kp:.1%} (expect ~47-53%)" if cov_kp is not None else "covered_kenpom: N/A")
    print(f"KenPom correct pick rate: {kp_correct:.1f}% (expect ~70-78%)" if kp_correct is not None else "KenPom correct pick: N/A")
    if not (0.47 <= cov_vegas <= 0.53):
        print("\nWARNING: covered_vegas rate outside 47-53% range!")
    print(f"\nSaved {len(df)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
