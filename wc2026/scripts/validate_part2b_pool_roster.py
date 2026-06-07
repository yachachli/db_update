"""Part 2B verification: SCO/COD/JOR roster fix + unchanged-prediction guard."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.database import get_connection  # noqa: E402
from src.math_utils import compute_baseline_goals  # noqa: E402
from src.pipeline import (  # noqa: E402
    _DEFAULT_CACHE_PATH,
    _bootstrap_missing_roster_teams,
    _load_pool_if_fresh,
    _resolve_team_by_id,
    _save_pool,
    bootstrap_tournament_pool,
    predict_matchup_by_id,
)

TARGET_FIXTURES = (8, 21, 22, 31, 45, 49, 52, 71, 73)
NEW_TEAM_IDS = (18706, 18552, 18559)


def _core_vals(row: dict) -> dict:
    return {
        "xg_a": float(row["xg_a"]),
        "xg_b": float(row["xg_b"]),
        "prob_a_win": float(row["prob_a_win"]),
        "prob_draw": float(row["prob_draw"]),
        "prob_b_win": float(row["prob_b_win"]),
    }


def _report_core(report: dict) -> dict:
    pred = report.get("prediction") or {}
    xg = pred.get("expected_goals") or {}
    probs = pred.get("win_probabilities") or {}
    return {
        "xg_a": float(xg.get("team_a", 0) or 0),
        "xg_b": float(xg.get("team_b", 0) or 0),
        "prob_a_win": float(probs.get("team_a_win", 0) or 0),
        "prob_draw": float(probs.get("draw", 0) or 0),
        "prob_b_win": float(probs.get("team_b_win", 0) or 0),
    }


def _is_degenerate(vals: dict) -> bool:
    return vals["xg_a"] == 0.0 and vals["xg_b"] == 0.0


def _load_predictions() -> dict[int, dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT fixture_id, team_a_id, team_b_id, xg_a, xg_b,
                   prob_a_win, prob_draw, prob_b_win
            FROM predictions
            """
        ).fetchall()
    return {int(r["fixture_id"]): dict(r) for r in rows}


def _load_fixture(fid: int) -> dict:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT fixture_id, team_a_id, team_b_id, team_a_name, team_b_name
            FROM wc2026_fixtures WHERE fixture_id = %s
            """,
            (fid,),
        ).fetchone()
    if not row:
        raise RuntimeError(f"fixture {fid} not found")
    return dict(row)


def _pool_without_new_teams(pool):
    """Simulate pre-2B pool: drop SCO/COD/JOR from predictable set, recompute baseline."""
    matches_by_team = {
        tid: ms for tid, ms in pool.matches_by_team.items()
        if tid not in NEW_TEAM_IDS
    }
    all_matches = [m for ms in matches_by_team.values() for m in ms]
    baseline = compute_baseline_goals(all_matches, pool.teams)
    host_ratings = {
        tid: r for tid, r in pool.host_ratings.items()
        if tid not in NEW_TEAM_IDS
    }
    return replace(
        pool,
        matches_by_team=matches_by_team,
        all_matches=all_matches,
        baseline=baseline,
        host_ratings=host_ratings,
    )


def main() -> int:
    print("=" * 78)
    print("PART 2B VERIFICATION — SCO / COD / JOR roster fix")
    print("=" * 78)

    before = _load_predictions()
    print(f"\nBaseline predictions in Neon: {len(before)}")

    cache_path = _PROJECT_ROOT / _DEFAULT_CACHE_PATH
    cached = _load_pool_if_fresh(cache_path, ttl_hours=24 * 365)
    if cached is None:
        print("No pool cache — running bootstrap (force_refresh=True)...")
        cached = bootstrap_tournament_pool(force_refresh=True)
    _save_pool(cached, cache_path)

    pre_pool = _pool_without_new_teams(cached)
    print(
        f"\nPre-2B equivalent pool: {len(pre_pool.matches_by_team)} predictable teams "
        f"(baseline frozen for incremental merge test)"
    )

    pool = _bootstrap_missing_roster_teams(pre_pool)
    _save_pool(pool, cache_path)
    print(f"Post-merge pool: {len(pool.matches_by_team)} predictable teams")

    for tid, code in zip(NEW_TEAM_IDS, ("SCO", "COD", "JOR")):
        status = "OK" if tid in pool.matches_by_team else "MISSING"
        try:
            _resolve_team_by_id(tid, pool)
            resolve = "predictable"
        except ValueError as exc:
            resolve = str(exc)
        print(f"  {code} ({tid}): matches_by_team={status}, resolve={resolve}")

    print("\n--- 9 TARGET FIXTURES ---")
    degenerate: list[int] = []
    failed: list[int] = []

    for fid in TARGET_FIXTURES:
        fx = _load_fixture(fid)
        a_id, b_id = int(fx["team_a_id"]), int(fx["team_b_id"])
        try:
            _resolve_team_by_id(a_id, pool)
            _resolve_team_by_id(b_id, pool)
        except ValueError as exc:
            print(f"  fixture {fid}: RESOLVE FAIL — {exc}")
            failed.append(fid)
            continue

        vals = _report_core(json.loads(predict_matchup_by_id(a_id, b_id, pool)))
        tag = ""
        if _is_degenerate(vals):
            tag = " *** DEGENERATE (data-coverage gap — not a real prediction) ***"
            degenerate.append(fid)

        print(
            f"  fixture {fid}: {fx['team_a_name']} vs {fx['team_b_name']} "
            f"xG={vals['xg_a']:.2f}/{vals['xg_b']:.2f} "
            f"probs=({vals['prob_a_win']:.3f},{vals['prob_draw']:.3f},"
            f"{vals['prob_b_win']:.3f}){tag}"
        )

    print("\n--- UNCHANGED-PREDICTION GUARD (pre-merge vs post-merge, frozen baseline) ---")
    mismatches: list[int] = []
    checked = 0
    with get_connection() as conn:
        group_rows = conn.execute(
            "SELECT fixture_id FROM wc2026_fixtures WHERE fixture_id < 74 ORDER BY fixture_id"
        ).fetchall()
    all_fids = [int(r["fixture_id"]) for r in group_rows]

    for fid in all_fids:
        if fid in TARGET_FIXTURES:
            continue
        fx = _load_fixture(fid)
        a_id, b_id = int(fx["team_a_id"]), int(fx["team_b_id"])
        try:
            _resolve_team_by_id(a_id, pre_pool)
            _resolve_team_by_id(b_id, pre_pool)
        except ValueError:
            continue
        prior = _report_core(json.loads(predict_matchup_by_id(a_id, b_id, pre_pool)))
        after = _report_core(json.loads(predict_matchup_by_id(a_id, b_id, pool)))
        checked += 1
        if after != prior:
            mismatches.append(fid)
            print(f"  fixture {fid}: CHANGED pre={prior} post={after}")

    if mismatches:
        print(f"\nFAIL: {len(mismatches)} fixture(s) changed after merge: {mismatches}")
    else:
        print(
            f"\nOK: {checked} non-target fixtures identical pre-merge vs post-merge "
            f"(adding SCO/COD/JOR did not perturb existing predictions)"
        )

    print("\n--- PLACEHOLDER SKIP EXIT-0 (cron subprocess) ---")
    env = {**os.environ, "REFRESH_WITHIN_DAYS": "365"}
    proc = subprocess.run(
        [sys.executable, str(_PROJECT_ROOT / "scripts" / "cron_refresh_predictions.py")],
        cwd=_PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    for line in (proc.stdout + proc.stderr).splitlines():
        if "RUN SUMMARY" in line or line.strip().endswith("exit=0"):
            print(f"  {line}")
    print(f"  cron exit code: {proc.returncode}")

    print("\n" + "=" * 78)
    print("SUMMARY")
    print("=" * 78)
    print(f"  9 fixtures resolve+predict: {len(TARGET_FIXTURES) - len(failed)}/{len(TARGET_FIXTURES)}")
    print(f"  degenerate flagged: {degenerate or 'none'}")
    print(f"  unchanged guard: {'PASS' if not mismatches else 'FAIL'}")
    print(f"  placeholder cron exit 0: {'PASS' if proc.returncode == 0 else 'FAIL'}")

    ok = not failed and not mismatches and proc.returncode == 0
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
