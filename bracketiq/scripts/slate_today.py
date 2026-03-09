"""
BracketIQ — Today's slate: current odds + predictions for moneyline, spread, and over/under.

- Spread & O/U: Use KenPom FanMatch (scraped daily) when we have a match; fallback to our formula.
- Game winner (moneyline): Our model only — recency-adjusted margin -> win prob vs Vegas.

Fetches live odds, loads today's FanMatch when available, computes edges. Only includes games
where both teams resolve to KenPom; flags suspicious edges.
Usage: py -m scripts.slate_today
Requires: ODDS_API_KEY in .env, KenPom cache (pomeroy_ratings_*.parquet). Run collect_historical_fanmatch --today-only for KenPom spread/O/U.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import norm

_backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend_root))

# KenPom-style constants (HCA 5.0 from 2026 season data: actual avg home margin +5.24)
HOME_COURT_ADVANTAGE = 5.0
MARGIN_SIGMA = 11.5

# Sanity caps: avoid crazy spreads from formula/name-resolution errors
MAX_PREDICTED_MARGIN = 28.0   # clamp |margin| to this before edge/win_prob
MAX_VEGAS_SPREAD_ABS = 35.0   # skip games with |vegas_spread| > this (bad data)
RECENCY_WEIGHT = 0.15         # light blend: recency adds at most ~±3 pts when one team hot/cold
RECENCY_CAP_PTS = 3.0         # clamp recency adjustment to ± this

# Sanity check vs KenPom FanMatch: flag if our margin/total not adjacent to KenPom's
SANITY_MARGIN_TOLERANCE = 5.0   # max |our_margin - kp_margin| to consider "adjacent"
SANITY_TOTAL_TOLERANCE = 8.0    # max |our_total - kp_total| to consider "adjacent"

# Edge confidence tiers from historical analysis (cover rates by |edge| bucket)
EDGE_COVER_RATES = {"NO_EDGE": "50.7%", "MILD": "52.8%", "STRONG": "54.3%", "HIGH_CONVICTION": "66.4%"}


def get_edge_confidence(edge: float) -> str:
    """Classify spread edge for display; based on historical cover rates by |edge|."""
    abs_edge = abs(edge) if edge is not None else 0.0
    if abs_edge < 1.0:
        return "NO_EDGE"
    if abs_edge < 3.0:
        return "MILD"
    if abs_edge < 5.0:
        return "STRONG"
    return "HIGH_CONVICTION"


def _get_rating(df: pd.DataFrame | None, team: str, col: str, default: float = 100.0) -> float:
    """Look up team in KenPom cache using central resolver (exact + aliases)."""
    from app.services.team_name_resolver import get_rating as resolver_get_rating
    return resolver_get_rating(df, team, col, default)


def _recency_adjustment(home_kp: str, away_kp: str, pomeroy: pd.DataFrame, window_days: int = 21) -> float:
    """Light recency: (home recent_margin_vs_expected - away) * weight, capped. Returns 0 if no schedule data."""
    try:
        from app.services.schedule_service import get_team_schedule
        from app.services.recency_service import calculate_recency_metrics
    except Exception:
        return 0.0
    home_sched = get_team_schedule(home_kp)
    away_sched = get_team_schedule(away_kp)
    if home_sched is None or away_sched is None:
        return 0.0
    home_rec = calculate_recency_metrics(home_kp, home_sched, pomeroy, window_days=window_days)
    away_rec = calculate_recency_metrics(away_kp, away_sched, pomeroy, window_days=window_days)
    if home_rec is None or away_rec is None:
        return 0.0
    home_mve = home_rec.get("recent_margin_vs_expected") or 0.0
    away_mve = away_rec.get("recent_margin_vs_expected") or 0.0
    adj = RECENCY_WEIGHT * (float(home_mve) - float(away_mve))
    return max(-RECENCY_CAP_PTS, min(RECENCY_CAP_PTS, adj))


def load_pomeroy() -> pd.DataFrame | None:
    from app.config import get_cache_dir
    cache_dir = get_cache_dir()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    files = list(cache_dir.glob("pomeroy_ratings_*.parquet"))
    if not files:
        return None
    return pd.read_parquet(max(files, key=lambda p: p.stat().st_mtime))


def _load_today_fanmatch() -> list[dict]:
    """Load FanMatch rows for today from parquet. Returns list of {home_canon, away_canon, kp_margin_home_pov, kp_total}."""
    from datetime import datetime, timezone
    from app.config import get_historical_dir
    from app.services.schedule_service import parse_fanmatch_game
    from app.services.team_name_resolver import resolve_to_canonical_kenpom
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist_dir = get_historical_dir()
    if not hist_dir.is_absolute():
        hist_dir = Path.cwd() / hist_dir
    fm_path = hist_dir / "fanmatch_2026.parquet"
    if not fm_path.exists():
        return []
    df = pd.read_parquet(fm_path)
    if "fanmatch_date" not in df.columns or "Game" not in df.columns:
        return []
    day = df[df["fanmatch_date"].astype(str).str[:10] == today]
    if day.empty:
        return []
    out = []
    for _, row in day.iterrows():
        parsed = parse_fanmatch_game(str(row.get("Game", "")))
        if not parsed:
            continue
        home = (parsed.get("home_team") or "").strip()
        away = (parsed.get("away_team") or "").strip()
        if not home or not away:
            continue
        home_canon = resolve_to_canonical_kenpom(home)
        away_canon = resolve_to_canonical_kenpom(away)
        pred_mov = row.get("PredictedMOV")
        pred_winner = (row.get("PredictedWinner") or "").strip()
        if pred_mov is None or pd.isna(pred_mov):
            continue
        pred_mov = float(pred_mov)
        pred_winner_canon = resolve_to_canonical_kenpom(pred_winner)
        kp_margin_home_pov = pred_mov if (pred_winner_canon and pred_winner_canon.lower() == home_canon.lower()) else -pred_mov
        kp_total = None
        pred_str = row.get("Prediction") or row.get("PredictedScore")
        if pred_str is not None and not pd.isna(pred_str):
            from app.services.schedule_service import parse_fanmatch_prediction
            parsed_pred = parse_fanmatch_prediction(str(pred_str))
            if parsed_pred and "predicted_score_fav" in parsed_pred and "predicted_score_dog" in parsed_pred:
                kp_total = float(parsed_pred["predicted_score_fav"]) + float(parsed_pred["predicted_score_dog"])
        out.append({"home_canon": home_canon, "away_canon": away_canon, "kp_margin_home_pov": kp_margin_home_pov, "kp_total": kp_total})
    return out


def _sanity_check_vs_kenpom(parsed: list[dict], fm_games: list[dict]) -> tuple[list[dict], list[dict]]:
    """Compare our predicted margin/total to KenPom FanMatch. Returns (updated games with sanity fields, list of warning dicts)."""
    from app.services.team_name_resolver import resolve_to_canonical_kenpom
    key_to_fm = {(g["home_canon"].lower(), g["away_canon"].lower()): g for g in fm_games}
    warnings = []
    for game in parsed:
        home_c = (game.get("home_team_kenpom") or "").strip()
        away_c = (game.get("away_team_kenpom") or "").strip()
        home_canon = resolve_to_canonical_kenpom(home_c) or home_c
        away_canon = resolve_to_canonical_kenpom(away_c) or away_c
        key = (home_canon.lower(), away_canon.lower())
        fm = key_to_fm.get(key)
        our_margin = game.get("kenpom_predicted_margin_home_pov")
        our_total = game.get("kenpom_predicted_total")
        if fm is None:
            game["kenpom_fanmatch_margin_home_pov"] = None
            game["kenpom_fanmatch_total"] = None
            game["sanity_adjacent"] = None
            game["sanity_note"] = "No FanMatch row for today"
            continue
        kp_margin = fm["kp_margin_home_pov"]
        kp_total = fm.get("kp_total")
        game["kenpom_fanmatch_margin_home_pov"] = round(kp_margin, 2)
        game["kenpom_fanmatch_total"] = round(kp_total, 1) if kp_total is not None else None
        margin_ok = (our_margin is not None and abs(float(our_margin) - kp_margin) <= SANITY_MARGIN_TOLERANCE)
        total_ok = True
        if kp_total is not None and our_total is not None:
            total_ok = abs(float(our_total) - kp_total) <= SANITY_TOTAL_TOLERANCE
        elif kp_total is not None or our_total is not None:
            total_ok = None
        game["sanity_adjacent"] = margin_ok and (total_ok is True or total_ok is None)
        note_parts = []
        if not margin_ok:
            note_parts.append(f"margin diff |our={our_margin} - kp={kp_margin}| = {abs(float(our_margin or 0) - kp_margin):.1f} > {SANITY_MARGIN_TOLERANCE}")
        if total_ok is False:
            note_parts.append(f"total diff |our={our_total} - kp={kp_total}| = {abs(float(our_total or 0) - (kp_total or 0)):.1f} > {SANITY_TOTAL_TOLERANCE}")
        game["sanity_note"] = "; ".join(note_parts) if note_parts else "Adjacent to KenPom"
        if note_parts:
            warnings.append({
                "away_team": away_c,
                "home_team": home_c,
                "our_margin": our_margin,
                "kenpom_margin": kp_margin,
                "our_total": our_total,
                "kenpom_total": kp_total,
                "note": game["sanity_note"],
            })
    return parsed, warnings


def parse_moneyline(game: dict) -> tuple[float | None, float | None]:
    """Get consensus implied prob for home and away from h2h market. Returns (prob_home, prob_away) or (None, None)."""
    home_team = game.get("home_team", "")
    away_team = game.get("away_team", "")
    home_odds_list = []
    away_odds_list = []
    for bk in game.get("bookmakers", []):
        for m in bk.get("markets", []):
            if m.get("key") != "h2h":
                continue
            for o in m.get("outcomes", []):
                name = o.get("name", "")
                price = o.get("price")
                if price is None:
                    continue
                try:
                    price = int(price)
                except (TypeError, ValueError):
                    continue
                if name == home_team:
                    home_odds_list.append(price)
                elif name == away_team:
                    away_odds_list.append(price)
    if not home_odds_list or not away_odds_list:
        return None, None
    from app.scrapers.odds_scraper import calculate_implied_probability
    home_probs = [calculate_implied_probability(p) for p in home_odds_list]
    away_probs = [calculate_implied_probability(p) for p in away_odds_list]
    return sum(home_probs) / len(home_probs), sum(away_probs) / len(away_probs)


def main() -> int:
    from app.scrapers.odds_scraper import get_current_odds, parse_game_odds
    from app.services.team_name_resolver import resolve_odds_to_kenpom_verified
    pomeroy = load_pomeroy()
    if pomeroy is None:
        print("KenPom cache not found (pomeroy_ratings_*.parquet). Run KenPom scrape first.")
        return 1
    try:
        games_raw = get_current_odds()
    except Exception as e:
        print(f"Failed to fetch current odds: {e}")
        return 1
    if not games_raw:
        print("No games returned from odds API.")
        return 0
    from app.services.team_name_resolver import find_team_row, resolve_to_canonical_kenpom
    # FanMatch for today: use KenPom's margin/total for spread & O/U when we have a match
    fm_today = _load_today_fanmatch()
    fm_lookup = {}
    if fm_today:
        for row in fm_today:
            key = (row["home_canon"].lower(), row["away_canon"].lower())
            fm_lookup[key] = row
    # Precompute tempo rank by cache Team name (1=fastest, higher=slower; bottom 100 = rank > 265)
    if "AdjT" in pomeroy.columns and "Team" in pomeroy.columns:
        _tr = pomeroy[["Team", "AdjT"]].copy()
        _tr["_tempo_rank"] = _tr["AdjT"].rank(method="min").astype(int)
        tempo_ranks = dict(zip(_tr["Team"].astype(str), _tr["_tempo_rank"]))
    else:
        tempo_ranks = {}
    parsed = []
    skipped_unresolved = 0
    skipped_extreme_spread = 0
    for g in games_raw:
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
            skipped_unresolved += 1
            continue
        vegas_spread = float(po["consensus_spread"])
        if abs(vegas_spread) > MAX_VEGAS_SPREAD_ABS:
            skipped_extreme_spread += 1
            continue
        vegas_total = float(po["consensus_total"]) if po.get("consensus_total") is not None else None
        prob_home_vegas, prob_away_vegas = parse_moneyline(g)
        adjO_h = _get_rating(pomeroy, home_kp, "AdjO", 100.0)
        adjD_h = _get_rating(pomeroy, home_kp, "AdjD", 100.0)
        adjT_h = _get_rating(pomeroy, home_kp, "AdjT", 67.0)
        adjO_a = _get_rating(pomeroy, away_kp, "AdjO", 100.0)
        adjD_a = _get_rating(pomeroy, away_kp, "AdjD", 100.0)
        adjT_a = _get_rating(pomeroy, away_kp, "AdjT", 67.0)
        tempo = (adjT_h + adjT_a) / 2.0
        # Never use raw per-100 as point margin (causes huge wrong edges when tempo is 0/missing)
        tempo_pts = tempo if tempo and float(tempo) > 0 else 67.0
        # Our model (with recency): used only for game winner / moneyline
        eff_margin_per_100 = (adjO_h - adjD_a) - (adjO_a - adjD_h) + HOME_COURT_ADVANTAGE
        our_margin = eff_margin_per_100 * (tempo_pts / 100.0)
        recency_adj = _recency_adjustment(home_kp, away_kp, pomeroy)
        our_margin = our_margin + recency_adj
        our_margin = max(-MAX_PREDICTED_MARGIN, min(MAX_PREDICTED_MARGIN, our_margin))
        win_prob_home = float(norm.cdf(our_margin / MARGIN_SIGMA))
        moneyline_edge = (win_prob_home - prob_home_vegas) if prob_home_vegas is not None else None
        # Spread & O/U: prefer KenPom FanMatch when we have a match; else our formula.
        # For neutral-site games Odds API may list home/away opposite to FanMatch (e.g. FanMatch
        # "Oregon St vs Gonzaga" = away, home; API may say home=Oregon St). Try both orderings.
        home_canon = resolve_to_canonical_kenpom(home_kp).lower()
        away_canon = resolve_to_canonical_kenpom(away_kp).lower()
        fm_key = (home_canon, away_canon)
        fm_key_rev = (away_canon, home_canon)
        fm = fm_lookup.get(fm_key)
        reversed_fm = False
        if fm is None:
            fm = fm_lookup.get(fm_key_rev)
            reversed_fm = fm is not None
        if fm is not None:
            # FanMatch margin is home POV; if we matched on (away, home), our "home" is FanMatch's away → negate
            margin_for_spread = fm["kp_margin_home_pov"] if not reversed_fm else -fm["kp_margin_home_pov"]
            predicted_total = fm.get("kp_total")
            spread_source = "kenpom_fanmatch"
            ou_source = "kenpom_fanmatch" if predicted_total is not None else None
        else:
            margin_for_spread = our_margin
            predicted_total = tempo_pts * (adjO_h + adjO_a) / 100.0
            spread_source = "our_model"
            ou_source = "our_model" if predicted_total is not None else None
        spread_edge = margin_for_spread + vegas_spread
        ou_edge = (predicted_total - vegas_total) if (predicted_total is not None and vegas_total is not None) else None
        edge_conf = get_edge_confidence(spread_edge)
        home_row = find_team_row(pomeroy, home_kp)
        home_cache_name = str(home_row.get("Team", home_kp)) if home_row is not None else home_kp
        home_tempo_rank = int(tempo_ranks.get(home_cache_name, 999))
        slow_underdog = (home_tempo_rank > 265 and vegas_spread > 0)
        parsed.append({
            "away_team": away_team,
            "home_team": home_team,
            "away_team_kenpom": away_kp,
            "home_team_kenpom": home_kp,
            "vegas_spread_home_pov": round(vegas_spread, 2),
            "vegas_total": round(vegas_total, 1) if vegas_total is not None else None,
            "vegas_implied_prob_home": round(prob_home_vegas, 4) if prob_home_vegas is not None else None,
            "vegas_implied_prob_away": round(prob_away_vegas, 4) if prob_away_vegas is not None else None,
            "recency_adjustment_pts": round(recency_adj, 2),
            "spread_source": spread_source,
            "ou_source": ou_source,
            "kenpom_predicted_margin_home_pov": round(margin_for_spread, 2),
            "kenpom_predicted_total": round(predicted_total, 1) if predicted_total is not None else None,
            "model_win_prob_home": round(win_prob_home, 4),
            "kenpom_win_prob_home": round(win_prob_home, 4),
            "spread_edge": round(spread_edge, 2),
            "spread_edge_confidence": edge_conf,
            "historical_cover_rate": EDGE_COVER_RATES.get(edge_conf, ""),
            "moneyline_edge": round(moneyline_edge, 4) if moneyline_edge is not None else None,
            "over_under_edge": round(ou_edge, 2) if ou_edge is not None else None,
            "spread_edge_interpretation": "Model likes HOME vs spread" if spread_edge > 0 else "Model likes AWAY vs spread",
            "moneyline_edge_interpretation": "Model likes HOME ML" if (moneyline_edge is not None and moneyline_edge > 0) else ("Model likes AWAY ML" if (moneyline_edge is not None and moneyline_edge < 0) else None),
            "ou_edge_interpretation": "Model likes OVER" if (ou_edge is not None and ou_edge > 0) else ("Model likes UNDER" if (ou_edge is not None and ou_edge < 0) else None),
            "slow_underdog_flag": slow_underdog,
            "slow_underdog_note": "Slow underdogs cover 56.8% historically" if slow_underdog else None,
        })
    if skipped_unresolved:
        print(f"Skipped {skipped_unresolved} games (team name not found in KenPom cache).", file=sys.stderr)
    if skipped_extreme_spread:
        print(f"Skipped {skipped_extreme_spread} games (|vegas_spread| > {MAX_VEGAS_SPREAD_ABS}).", file=sys.stderr)
    if not parsed:
        print("No games with spread data.")
        return 0
    # Sanity check: compare our margin/total to KenPom FanMatch for today (when available)
    fm_today = _load_today_fanmatch()
    parsed, sanity_warnings = _sanity_check_vs_kenpom(parsed, fm_today)
    games_compared = sum(1 for g in parsed if g.get("kenpom_fanmatch_margin_home_pov") is not None)
    if fm_today:
        sanity_result = {
            "checked": True,
            "games_compared": games_compared,
            "games_on_slate": len(parsed),
            "warnings": sanity_warnings,
            "margin_tolerance_pts": SANITY_MARGIN_TOLERANCE,
            "total_tolerance_pts": SANITY_TOTAL_TOLERANCE,
        }
        for w in sanity_warnings:
            print(
                f"  SANITY: {w['away_team']} at {w['home_team']} — {w['note']}",
                file=sys.stderr,
            )
    else:
        sanity_result = {"checked": False, "reason": "No FanMatch data for today (run collect_historical_fanmatch to include today)"}
    # Validation: flag suspicious edges (likely name resolution errors)
    for game in parsed:
        edge = game.get("spread_edge") or 0
        margin = game.get("kenpom_predicted_margin_home_pov") or 0
        if abs(edge) > 15 or abs(margin) > 35:
            print(
                f"  SUSPICIOUS EDGE: {game.get('away_team_kenpom')} at {game.get('home_team_kenpom')}: "
                f"edge={edge:.1f}, margin={margin:.1f} (possible name resolution error)",
                file=sys.stderr,
            )
    by_spread = sorted(parsed, key=lambda x: (-abs(x["spread_edge"]), -abs(x.get("moneyline_edge") or 0)))
    by_moneyline = sorted(parsed, key=lambda x: (-abs(x.get("moneyline_edge") or 0), -abs(x["spread_edge"])))
    by_ou = sorted(parsed, key=lambda x: (-abs(x.get("over_under_edge") or 0), -abs(x["spread_edge"])))
    out = {
        "date": "today",
        "total_games": len(parsed),
        "data_sources": {
            "spread_and_ou": "KenPom FanMatch when game matches today's scrape; else our formula (fallback)",
            "game_winner_moneyline": "Our model only (recency-adjusted margin -> win prob vs Vegas)",
        },
        "sanity_check": sanity_result,
        "games_sorted_by_spread_edge": by_spread,
        "games_sorted_by_moneyline_edge": by_moneyline,
        "games_sorted_by_over_under_edge": by_ou,
        "all_games": parsed,
    }
    print(json.dumps(out, indent=2))
    out_dir = _backend_root / "data" / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "slate_today.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved to {path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
