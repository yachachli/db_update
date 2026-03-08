"""
BracketIQ data audit — diagnostic only. Run: py -m scripts.audit_data
Prints full diagnostics to console and saves data/analysis/audit_report.txt
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

_backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend_root))

import pandas as pd
import numpy as np


def _hist_dirs():
    for base in [_backend_root / "data" / "historical", _backend_root / "app" / "data" / "historical"]:
        if (base / "ats_complete_2026.parquet").exists():
            return base
    return _backend_root / "data" / "historical"


def _out(msg: str, buf: io.StringIO):
    print(msg)
    buf.write(msg + "\n")


def audit_ats_dataset(buf: io.StringIO):
    _out("\n" + "=" * 70, buf)
    _out("1A: AUDIT ats_complete_2026.parquet", buf)
    _out("=" * 70, buf)
    hist = _hist_dirs()
    path = hist / "ats_complete_2026.parquet"
    if not path.exists():
        _out(f"NOT FOUND: {path}", buf)
        return None
    df = pd.read_parquet(path)

    _out("\n1. Column names and dtypes:", buf)
    for c in df.columns:
        _out(f"   {c}: {df[c].dtype}", buf)

    _out("\n2. First 10 rows (full):", buf)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    _out(df.head(10).to_string(), buf)

    _out("\n3. describe() numeric:", buf)
    num = df.select_dtypes(include=[np.number])
    if not num.empty:
        _out(num.describe().to_string(), buf)

    _out("\n4. actual_margin_home: mean, median, min, max, std", buf)
    m = df["actual_margin_home"].dropna()
    if len(m):
        _out(f"   mean={m.mean():.2f} median={m.median():.2f} min={m.min():.2f} max={m.max():.2f} std={m.std():.2f}", buf)
        _out("   >>> Negative mean is dataset bias (games with odds skew to strong-away vs weak-home); not a bug.", buf)

    _out("\n5. vegas_spread: mean, median, min, max, std (Option A: positive = home getting points)", buf)
    v = df["vegas_spread"].dropna()
    if len(v):
        _out(f"   mean={v.mean():.2f} median={v.median():.2f} min={v.min():.2f} max={v.max():.2f} std={v.std():.2f}", buf)
        _out("   >>> Option A: negative = home favored; positive = home underdog. Sample where home big favorite:", buf)
        home_fav = df[df["vegas_spread"] < -8].head(5)
        _out(home_fav[["game_date", "home_team", "away_team", "vegas_spread", "actual_margin_home"]].to_string(), buf)
    if "home_away_aligned" in df.columns:
        aligned = df["home_away_aligned"].sum()
        flipped = (~df["home_away_aligned"]).sum()
        _out(f"   home/away: {int(aligned)} same (Odds=FM), {int(flipped)} flipped (spread sign corrected)", buf)

    _out("\n6. kenpom_predicted_margin: mean, median, min, max, std", buf)
    k = df["kenpom_predicted_margin"].dropna()
    if len(k):
        _out(f"   mean={k.mean():.2f} median={k.median():.2f} min={k.min():.2f} max={k.max():.2f} std={k.std():.2f}", buf)

    _out("\n7. covered_vegas: True vs False count", buf)
    cv = df["covered_vegas"].dropna()
    if len(cv):
        t, f = cv.sum(), (cv == False).sum()
        _out(f"   True={int(t)} False={int(f)} (expect ~50/50)", buf)

    _out("\n8. covered_kenpom: True vs False count", buf)
    ck = df["covered_kenpom"].dropna()
    if len(ck):
        t, f = ck.sum(), (ck == False).sum()
        _out(f"   True={int(t)} False={int(f)}", buf)

    _out("\n9. kenpom_vs_vegas_edge: positive vs negative count", buf)
    e = df["kenpom_vs_vegas_edge"].dropna()
    if len(e):
        pos = (e > 0).sum()
        neg = (e < 0).sum()
        _out(f"   positive={int(pos)} negative={int(neg)} (both should be non-zero)", buf)

    _out("\n10. Spot-check 5 games (Duke/Kansas/Kentucky etc.):", buf)
    for team in ["Duke", "Kansas", "Kentucky", "North Carolina", "UConn"]:
        sub = df[(df["home_team"].str.contains(team, na=False)) | (df["away_team"].str.contains(team, na=False))].head(1)
        if len(sub):
            _out(sub.to_string(), buf)
    return df


def audit_fanmatch(buf: io.StringIO):
    _out("\n" + "=" * 70, buf)
    _out("1B: AUDIT fanmatch_2026.parquet", buf)
    _out("=" * 70, buf)
    hist = _hist_dirs()
    path = hist / "fanmatch_2026.parquet"
    if not path.exists():
        _out(f"NOT FOUND: {path}", buf)
        return None
    df = pd.read_parquet(path)

    _out("\n1. Column names and dtypes:", buf)
    for c in df.columns:
        _out(f"   {c}: {df[c].dtype}", buf)

    _out("\n2. First 10 rows:", buf)
    _out(df.head(10).to_string(), buf)

    margin_cols = [c for c in df.columns if "margin" in c.lower() or "mov" in c.lower() or "score" in c.lower()]
    _out("\n3. Margin/score columns: " + str(margin_cols), buf)
    if margin_cols:
        _out(df[margin_cols + ["Game", "Winner", "PredictedWinner"]].head(10).to_string(), buf)

    _out("\n4. Game column sample (10):", buf)
    for g in df["Game"].dropna().head(10).tolist():
        _out(f"   {g}", buf)

    pred_col = "PredictedScore" if "PredictedScore" in df.columns else [c for c in df.columns if "pred" in c.lower()]
    _out("\n5. Prediction-related columns sample:", buf)
    _out(df[["Game", "PredictedWinner", "PredictedMOV", "PredictedLoser", "Winner", "ActualMOV", "Loser"]].head(10).to_string(), buf)

    _out("\n6. Perspective check: PredictedMOV vs PredictedWinner. When KenPom predicts away team, is PredictedMOV from away (winner) POV?", buf)
    return df


def audit_join(buf: io.StringIO):
    _out("\n" + "=" * 70, buf)
    _out("1C: AUDIT Join (FanMatch + Odds)", buf)
    _out("=" * 70, buf)
    hist = _hist_dirs()
    fm_path = hist / "fanmatch_2026.parquet"
    od_path = hist / "odds_2026.parquet"
    if not fm_path.exists() or not od_path.exists():
        _out("Missing fanmatch or odds parquet.", buf)
        return
    fm = pd.read_parquet(fm_path)
    od = pd.read_parquet(od_path)
    from app.services.schedule_service import parse_fanmatch_game
    dates = fm["fanmatch_date"].dropna().unique()[:5]
    for d in dates:
        dstr = str(d)[:10]
        _out(f"\n  Date {dstr}:", buf)
        fm_d = fm[fm["fanmatch_date"].astype(str).str[:10] == dstr]
        od_d = od[od["game_date"] == dstr]
        _out(f"    FanMatch rows: {len(fm_d)}, Odds rows: {len(od_d)}", buf)
        for _, r in fm_d.head(2).iterrows():
            g = r.get("Game")
            if pd.isna(g):
                continue
            p = parse_fanmatch_game(str(g))
            if p:
                _out(f"    FM Game: {g} -> home={p['home_team']} away={p['away_team']}", buf)
                match = od_d[((od_d["home_team_kenpom"] == p["home_team"]) & (od_d["away_team_kenpom"] == p["away_team"])) |
                           ((od_d["home_team_kenpom"] == p["away_team"]) & (od_d["away_team_kenpom"] == p["home_team"]))]
                _out(f"    Odds match: {len(match)} rows. Sample: home_team_kenpom, away_team_kenpom, consensus_spread", buf)
                if len(match):
                    _out(match[["home_team_kenpom", "away_team_kenpom", "consensus_spread"]].head(2).to_string(), buf)


def audit_analysis_logic(buf: io.StringIO):
    _out("\n" + "=" * 70, buf)
    _out("1D: AUDIT model_analysis logic", buf)
    _out("=" * 70, buf)
    hist = _hist_dirs()
    path = hist / "ats_complete_2026.parquet"
    if not path.exists():
        _out("ATS parquet not found.", buf)
        return
    df = pd.read_parquet(path)

    _out("\n1. KenPom correct pick: (sign(kenpom_predicted_margin) == sign(actual_margin_home)). Both in home POV.", buf)
    df["pred_winner_home"] = np.sign(df["kenpom_predicted_margin"]) > 0
    df["actual_winner_home"] = df["actual_margin_home"] > 0
    df["correct_pick"] = df["pred_winner_home"] == df["actual_winner_home"]
    correct = df["correct_pick"].mean() * 100
    _out(f"   Correct pick rate: {correct:.1f}% (expect 70-78%)", buf)
    _out("   10 sample rows: home_team, away_team, kenpom_predicted_margin, actual_margin_home, pred_winner_home, actual_winner_home, correct_pick", buf)
    _out(df[["home_team", "away_team", "kenpom_predicted_margin", "actual_margin_home", "pred_winner_home", "actual_winner_home", "correct_pick"]].head(10).to_string(), buf)

    _out("\n2. kenpom_vs_vegas_edge = kenpom_predicted_margin (home POV) + vegas_spread. Positive = KenPom likes home more.", buf)
    _out("   Edge distribution:", buf)
    _out(f"   min={df['kenpom_vs_vegas_edge'].min():.2f} max={df['kenpom_vs_vegas_edge'].max():.2f} mean={df['kenpom_vs_vegas_edge'].mean():.2f}", buf)
    neg = (df["kenpom_vs_vegas_edge"] < 0).sum()
    pos = (df["kenpom_vs_vegas_edge"] > 0).sum()
    _out(f"   Count edge < 0: {neg}, edge > 0: {pos} (both should be non-zero)", buf)
    _out("\n3. If vegas_spread is 0 for all rows, odds data is wrong; re-run collect_historical_odds or check API.", buf)

    _out("\n4. Spread profiles: require kenpom_df for ranks/tempo. If KenPom cache missing or team names don't match, rows dropped.", buf)
    _out("\n5. Upset patterns: similar; check filter logic.", buf)

    _out("\n--- VALIDATION CHECKLIST (after fix) ---", buf)
    am = df["actual_margin_home"].dropna()
    _out(f"  actual_margin_home mean: {am.mean():.2f} (expect +2 to +5 if home/away correct; negative may mean result order is away,home)", buf)
    _out(f"  KenPom correct pick %: {correct:.1f}% (expect 70-78%)", buf)
    _out(f"  covered_vegas True%: {df['covered_vegas'].mean()*100:.1f}% (expect ~50; broken if vegas_spread all 0)", buf)
    _out(f"  covered_kenpom True%: {df['covered_kenpom'].dropna().mean()*100:.1f}% (expect ~48-54%)", buf)
    kpm = df["kenpom_predicted_margin"].dropna()
    mae = (df["actual_margin_home"] - df["kenpom_predicted_margin"]).abs().mean()
    _out(f"  MAE(actual_margin_home, kenpom_predicted_margin): {mae:.2f} (expect 8.5-10.5)", buf)


def main():
    buf = io.StringIO()
    _out("BRACKETIQ DATA AUDIT", buf)
    audit_ats_dataset(buf)
    audit_fanmatch(buf)
    audit_join(buf)
    audit_analysis_logic(buf)

    out_dir = _backend_root / "data" / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "audit_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(buf.getvalue())
    _out(f"\n\nAudit report saved to {report_path}", buf)
    return 0


if __name__ == "__main__":
    sys.exit(main())
