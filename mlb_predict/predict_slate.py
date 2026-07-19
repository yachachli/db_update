"""Generate predictions for an MLB slate and upsert them into Neon.

VENDORED COPY — source of truth is the propgpt-mlb repo
(scripts/predict_slate.py + src/propgpt_mlb/). This copy lives in db_update
because GitHub Actions runs on this repo's account; keep the two in sync when
the model or feature code changes (see mlb_predict/README.md).

Loads trained totals (Ridge) + moneyline (Logit) pipelines, builds point-in-time
features for the slate date, and writes one prediction object per game.

Usage (from the mlb_predict/ directory):
    python predict_slate.py --date 2026-07-12 --write-db
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd

from propgpt_mlb.db import SCHEMA, fetch_all, get_connection
from propgpt_mlb.features.build import (
    ML_FEATURE_COLS,
    TOTALS_FEATURE_COLS,
    build_features_for_games,
    features_json,
)
from propgpt_mlb.features.odds import american_to_implied_prob

MODELS_DIR = Path(__file__).resolve().parent / "models"
DEFAULT_TOTALS = MODELS_DIR / "totals_multi.joblib"
DEFAULT_ML = MODELS_DIR / "ml_multi.joblib"
MODEL_VERSION = "v2_ridge_logit_202507"
PREFERRED_BOOKS = ("draftkings", "fanduel", "betmgm", "caesars")


def _load_slate_games(slate_date: str) -> pd.DataFrame:
    rows = fetch_all(
        f"""
        SELECT
            g.game_id,
            g.game_date,
            g.season,
            g.game_time_utc,
            g.status,
            g.home_team_id,
            g.away_team_id,
            g.park_id,
            g.home_sp_id,
            g.away_sp_id,
            ht.abbr AS home_abbr,
            at.abbr AS away_abbr,
            ht.name AS home_name,
            at.name AS away_name,
            p.name AS park_name,
            p.is_dome,
            hp.full_name AS home_sp_name,
            ap.full_name AS away_sp_name,
            hp.throws AS home_sp_throws,
            ap.throws AS away_sp_throws
        FROM {SCHEMA}.games g
        JOIN {SCHEMA}.teams ht ON g.home_team_id = ht.team_id
        JOIN {SCHEMA}.teams at ON g.away_team_id = at.team_id
        LEFT JOIN {SCHEMA}.parks p ON g.park_id = p.park_id
        LEFT JOIN {SCHEMA}.players hp ON g.home_sp_id = hp.player_id
        LEFT JOIN {SCHEMA}.players ap ON g.away_sp_id = ap.player_id
        WHERE g.game_date = %s
        ORDER BY g.game_time_utc NULLS LAST, g.game_id
        """,
        (slate_date,),
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["game_date"] = pd.to_datetime(df["game_date"])
    return df


def _latest_odds_for_games(game_ids: list[int]) -> dict[int, dict]:
    if not game_ids:
        return {}
    placeholders = ",".join(["%s"] * len(game_ids))
    book_order = " ".join(
        f"WHEN o.book = '{b}' THEN {i}" for i, b in enumerate(PREFERRED_BOOKS)
    )
    # Pregame snapshots only: the odds cron also captures in-play lines once a
    # game has started (live totals/run lines), which must never be used as the
    # line-at-prediction.
    rows = fetch_all(
        f"""
        SELECT DISTINCT ON (o.game_id)
            o.game_id, o.book, o.total_line, o.over_odds, o.under_odds,
            o.ml_home, o.ml_away, o.snapshot_time, o.segment
        FROM {SCHEMA}.odds_snapshots o
        JOIN {SCHEMA}.games g ON g.game_id = o.game_id
        WHERE o.game_id IN ({placeholders})
          AND o.segment = 'full_game'
          AND (g.game_time_utc IS NULL OR o.snapshot_time <= g.game_time_utc)
        ORDER BY o.game_id,
                 CASE {book_order} ELSE 99 END,
                 o.snapshot_time DESC
        """,
        tuple(game_ids),
    )
    out: dict[int, dict] = {}
    for r in rows:
        out[int(r["game_id"])] = {
            "book": r["book"],
            "total_line": float(r["total_line"]) if r["total_line"] is not None else None,
            "over_odds": r["over_odds"],
            "under_odds": r["under_odds"],
            "ml_home": r["ml_home"],
            "ml_away": r["ml_away"],
            "mkt_home_prob": american_to_implied_prob(r["ml_home"]),
            "snapshot_time": r["snapshot_time"].isoformat() if r["snapshot_time"] else None,
        }
    return out


def _weather_for_games(game_ids: list[int]) -> dict[int, dict]:
    if not game_ids:
        return {}
    placeholders = ",".join(["%s"] * len(game_ids))
    rows = fetch_all(
        f"""
        SELECT game_id, temp_f, wind_mph, wind_dir_deg, precip_pct,
               humidity_pct, cloud_cover_pct, is_dome_game, observed_for_time
        FROM {SCHEMA}.weather_observations
        WHERE game_id IN ({placeholders})
        """,
        tuple(game_ids),
    )
    out: dict[int, dict] = {}
    for r in rows:
        out[int(r["game_id"])] = {
            "temp_f": float(r["temp_f"]) if r["temp_f"] is not None else None,
            "wind_mph": float(r["wind_mph"]) if r["wind_mph"] is not None else None,
            "wind_dir_deg": float(r["wind_dir_deg"]) if r["wind_dir_deg"] is not None else None,
            "precip_pct": float(r["precip_pct"]) if r["precip_pct"] is not None else None,
            "humidity_pct": float(r["humidity_pct"]) if r["humidity_pct"] is not None else None,
            "is_dome_game": bool(r["is_dome_game"]),
        }
    return out


def _edge_grade(edge: float | None) -> str | None:
    if edge is None:
        return None
    a = abs(edge)
    if a >= 1.0:
        return "strong"
    if a >= 0.5:
        return "mild"
    return "pass"


def predict_slate(
    slate_date: str,
    *,
    totals_path: Path,
    ml_path: Path,
) -> dict:
    games = _load_slate_games(slate_date)
    if games.empty:
        return {
            "slate_date": slate_date,
            "model_version": MODEL_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_games": 0,
            "games": [],
            "error": "No games found for this date — run mlb_games_update first",
        }

    # Only predict games with both SPs (feature builder requires them)
    predictible = games[games["home_sp_id"].notna() & games["away_sp_id"].notna()].copy()
    skipped = games[games["home_sp_id"].isna() | games["away_sp_id"].isna()]

    totals_model = joblib.load(totals_path)
    ml_model = joblib.load(ml_path)

    feat = build_features_for_games(predictible) if not predictible.empty else pd.DataFrame()
    odds = _latest_odds_for_games(games["game_id"].astype(int).tolist())
    weather = _weather_for_games(games["game_id"].astype(int).tolist())

    # Map game_id -> feature row
    feat_by_id = {int(r.game_id): r for r in feat.itertuples()} if not feat.empty else {}

    predictions = []
    for g in games.itertuples():
        gid = int(g.game_id)
        row_out: dict = {
            "game_id": gid,
            "game_date": slate_date,
            "game_time_utc": g.game_time_utc.isoformat() if g.game_time_utc else None,
            "status": g.status,
            "away_team": g.away_abbr,
            "home_team": g.home_abbr,
            "away_name": g.away_name,
            "home_name": g.home_name,
            "park": g.park_name,
            "away_sp": g.away_sp_name if pd.notna(g.away_sp_name) else None,
            "home_sp": g.home_sp_name if pd.notna(g.home_sp_name) else None,
            "away_sp_throws": g.away_sp_throws if pd.notna(g.away_sp_throws) else None,
            "home_sp_throws": g.home_sp_throws if pd.notna(g.home_sp_throws) else None,
        }

        fr = feat_by_id.get(gid)
        if fr is None:
            row_out["skip_reason"] = "missing_probable_pitcher"
            row_out["prediction"] = None
            predictions.append(row_out)
            continue

        X_tot = pd.DataFrame([{c: getattr(fr, c) for c in TOTALS_FEATURE_COLS}])
        X_ml = pd.DataFrame([{c: getattr(fr, c) for c in ML_FEATURE_COLS}])
        pred_total = float(totals_model.predict(X_tot)[0])
        p_home = float(ml_model.predict_proba(X_ml)[0, 1])

        mkt = odds.get(gid)
        line = mkt["total_line"] if mkt else None
        edge = (pred_total - line) if line is not None else None
        total_pick = None
        if edge is not None:
            if abs(edge) < 0.25:
                total_pick = "pass"
            else:
                total_pick = "over" if edge > 0 else "under"

        ml_pick = "home" if p_home >= 0.5 else "away"
        mkt_home = mkt["mkt_home_prob"] if mkt else None
        ml_edge = (p_home - mkt_home) if mkt_home is not None else None

        row_out["prediction"] = {
            "predicted_total": round(pred_total, 2),
            "p_home_win": round(p_home, 4),
            "p_away_win": round(1.0 - p_home, 4),
            "ml_pick": ml_pick,
            "total_pick": total_pick,
            "total_edge_runs": round(edge, 2) if edge is not None else None,
            "total_edge_grade": _edge_grade(edge),
            "ml_edge_prob": round(ml_edge, 4) if ml_edge is not None else None,
            "features": features_json(pd.Series({c: getattr(fr, c) for c in TOTALS_FEATURE_COLS + ML_FEATURE_COLS})),
        }
        row_out["market"] = mkt
        row_out["weather"] = weather.get(gid)
        predictions.append(row_out)

    return {
        "slate_date": slate_date,
        "model_version": MODEL_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "totals_model": str(totals_path.name),
        "ml_model": str(ml_path.name),
        "n_games": len(games),
        "n_predicted": len(predictible),
        "n_skipped": len(skipped),
        "games": predictions,
    }


# Interpretable subset of features surfaced as explanation_top_factors until
# the SHAP/LLM explanation layer exists.
_TOP_FACTOR_KEYS = (
    "expected_total",
    "sp_quality_sum",
    "home_sp_era_adj",
    "away_sp_era_adj",
    "park_run_factor",
    "weather_temp_f",
    "weather_wind_mph",
)


def _template_explanation(g: dict) -> str:
    """Deterministic one-line summary; replaced later by the LLM analysis layer."""
    pred = g["prediction"]
    mkt = g.get("market") or {}
    parts = [f"Model projects {pred['predicted_total']:.1f} total runs"]
    if mkt.get("total_line") is not None:
        parts.append(
            f"vs the {mkt['total_line']:g} line "
            f"({pred['total_pick']}, edge {pred['total_edge_runs']:+.1f} runs, "
            f"{pred['total_edge_grade']})"
        )
    fav = g["home_team"] if pred["ml_pick"] == "home" else g["away_team"]
    p = pred["p_home_win"] if pred["ml_pick"] == "home" else pred["p_away_win"]
    parts.append(f"and gives {fav} a {p:.0%} win probability")
    sps = []
    if g.get("away_sp"):
        sps.append(f"{g['away_sp']} ({g['away_team']})")
    if g.get("home_sp"):
        sps.append(f"{g['home_sp']} ({g['home_team']})")
    if sps:
        parts.append(f"with {' vs '.join(sps)} as probable starters")
    return " ".join(parts) + "."


def write_predictions_to_db(payload: dict) -> int:
    """Upsert slate predictions into predictions on (game_id, model_version).

    Games without a prediction (missing probable SP) are skipped. Returns the
    number of rows upserted.
    """
    rows = []
    for g in payload["games"]:
        pred = g.get("prediction")
        if not pred:
            continue
        mkt = g.get("market") or {}
        features = pred.get("features") or {}
        top_factors = {k: features.get(k) for k in _TOP_FACTOR_KEYS if k in features}
        rows.append((
            g["game_id"],
            payload["model_version"],
            pred["predicted_total"],
            pred["p_home_win"],
            mkt.get("total_line"),
            mkt.get("over_odds"),
            mkt.get("under_odds"),
            mkt.get("ml_home"),
            mkt.get("ml_away"),
            json.dumps(_json_safe(features), default=str),
            pred["total_pick"],
            pred["total_edge_grade"],
            pred["total_edge_runs"],
            pred["ml_pick"],
            round(pred["ml_edge_prob"] * 100, 2) if pred.get("ml_edge_prob") is not None else None,
            _template_explanation(g),
            json.dumps(_json_safe(top_factors), default=str),
        ))
    if not rows:
        return 0

    sql = f"""
        INSERT INTO {SCHEMA}.predictions (
            game_id, model_version, made_at,
            predicted_total, p_home_win,
            line_at_prediction_total, line_at_prediction_over_odds,
            line_at_prediction_under_odds, line_at_prediction_ml_home,
            line_at_prediction_ml_away,
            features_json,
            total_pick, total_grade, total_edge_runs,
            ml_pick, ml_edge_pct,
            explanation, explanation_top_factors
        ) VALUES (
            %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (game_id, model_version) DO UPDATE SET
            made_at = NOW(),
            predicted_total = EXCLUDED.predicted_total,
            p_home_win = EXCLUDED.p_home_win,
            line_at_prediction_total = EXCLUDED.line_at_prediction_total,
            line_at_prediction_over_odds = EXCLUDED.line_at_prediction_over_odds,
            line_at_prediction_under_odds = EXCLUDED.line_at_prediction_under_odds,
            line_at_prediction_ml_home = EXCLUDED.line_at_prediction_ml_home,
            line_at_prediction_ml_away = EXCLUDED.line_at_prediction_ml_away,
            features_json = EXCLUDED.features_json,
            total_pick = EXCLUDED.total_pick,
            total_grade = EXCLUDED.total_grade,
            total_edge_runs = EXCLUDED.total_edge_runs,
            ml_pick = EXCLUDED.ml_pick,
            ml_edge_pct = EXCLUDED.ml_edge_pct,
            explanation = EXCLUDED.explanation,
            explanation_top_factors = EXCLUDED.explanation_top_factors
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def _json_safe(obj):
    """Convert NaN/NaT to None for valid JSON."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if obj is None:
        return None
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    return obj


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict PropGPT MLB slate")
    parser.add_argument("--date", default=None, help="Slate date YYYY-MM-DD (default: tomorrow ET-ish = UTC today+1 if evening)")
    parser.add_argument("--out", default=None, help="Output JSON path")
    parser.add_argument("--totals-model", default=str(DEFAULT_TOTALS))
    parser.add_argument("--ml-model", default=str(DEFAULT_ML))
    parser.add_argument(
        "--write-db",
        action="store_true",
        help="Upsert predictions into predictions in addition to the JSON output",
    )
    args = parser.parse_args()

    if args.date:
        slate_date = args.date
    else:
        # Default: tomorrow's calendar date in US/Eastern
        from zoneinfo import ZoneInfo
        et = datetime.now(ZoneInfo("America/New_York")).date()
        from datetime import timedelta
        slate_date = (et + timedelta(days=1)).isoformat()

    totals_path = Path(args.totals_model)
    ml_path = Path(args.ml_model)
    if not totals_path.exists() or not ml_path.exists():
        print(f"Missing model files:\n  {totals_path}\n  {ml_path}", file=sys.stderr)
        raise SystemExit(1)

    print(f"Predicting slate {slate_date} ...")
    payload = predict_slate(slate_date, totals_path=totals_path, ml_path=ml_path)

    if args.out:
        out_path = Path(args.out)
    else:
        out_dir = Path(__file__).resolve().parent / "predictions"
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / f"slate_{slate_date}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(_json_safe(payload), indent=2, default=str),
        encoding="utf-8",
    )
    print(f"Wrote {out_path}")
    print(f"  games={payload['n_games']} predicted={payload['n_predicted']} skipped={payload['n_skipped']}")
    if payload.get("error"):
        # An empty slate is normal on off-days (All-Star break etc.), so a
        # scheduled run shouldn't go red — but surface it loudly for humans.
        print(f"  WARNING: {payload['error']}", file=sys.stderr)
        return

    if args.write_db:
        n = write_predictions_to_db(payload)
        print(f"  Upserted {n} rows into {SCHEMA}.predictions")

    # Compact console summary
    for g in payload["games"]:
        pred = g.get("prediction")
        if not pred:
            print(f"  SKIP {g['away_team']}@{g['home_team']}: {g.get('skip_reason')}")
            continue
        line = (g.get("market") or {}).get("total_line")
        line_bit = ""
        if line is not None:
            line_bit = f" (line {line}, {pred['total_pick']})"
        print(
            f"  {g['away_team']:>3}@{g['home_team']:<3}  "
            f"total={pred['predicted_total']:.1f}{line_bit}  "
            f"p_home={pred['p_home_win']:.1%} ({pred['ml_pick']})"
        )


if __name__ == "__main__":
    main()
