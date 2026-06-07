"""One-off Phase 4.3v cron E2E inspection (read-only Neon queries)."""

from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import (  # noqa: E402
    get_connection,
    get_fixtures_needing_prediction,
    get_player_ratings_snapshot_summary,
)


def _fixture_window_report() -> None:
    with get_connection() as conn:
        all_sched = conn.execute(
            """
            SELECT fixture_id, team_a_name, team_b_name, scheduled_at, status
            FROM wc2026_fixtures
            WHERE status = 'scheduled'
            ORDER BY scheduled_at
            LIMIT 20
            """
        ).fetchall()
        pred_ages = conn.execute(
            """
            SELECT p.fixture_id, p.predicted_at, f.scheduled_at,
                   EXTRACT(EPOCH FROM (NOW() - p.predicted_at)) / 3600 AS age_hours
            FROM predictions p
            JOIN wc2026_fixtures f ON f.fixture_id = p.fixture_id
            ORDER BY p.predicted_at DESC
            """
        ).fetchall()
    print("scheduled fixtures (first 20):")
    for r in all_sched:
        print(f"  {dict(r)}")
    print("\nprediction ages:")
    for r in pred_ages:
        print(f"  {dict(r)}")
    for days in (3, 30, 365):
        n = len(get_fixtures_needing_prediction(within_days=days))
        print(f"fixtures needing prediction within {days}d: {n}")


def main() -> int:
    today = date.today()
    label = sys.argv[1] if len(sys.argv) > 1 else "INSPECT"
    print("=" * 78)
    print(f"PHASE 4.3v E2E — {label}")
    print("=" * 78)

    with get_connection() as conn:
        pred_count = conn.execute("SELECT COUNT(*) AS n FROM predictions").fetchone()["n"]
        cols = conn.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'predictions' ORDER BY ordinal_position
            """
        ).fetchall()
        hist_today = conn.execute(
            "SELECT COUNT(*) AS n FROM player_ratings_history WHERE snapshot_date = %s",
            (today,),
        ).fetchone()["n"]
        hist_dates = conn.execute(
            """
            SELECT snapshot_date, COUNT(*) AS n, COUNT(DISTINCT team_code) AS teams
            FROM player_ratings_history
            GROUP BY snapshot_date
            ORDER BY snapshot_date DESC
            LIMIT 5
            """
        ).fetchall()
        source_totals = conn.execute(
            """
            SELECT source, COUNT(*) AS n, COUNT(DISTINCT team_code) AS teams
            FROM player_ratings_history
            WHERE snapshot_date = %s
            GROUP BY source
            ORDER BY source
            """,
            (today,),
        ).fetchall()
        arg_gonz = conn.execute(
            """
            SELECT entity_key, player_name, sportmonks_player_id, source, manual_squad_no
            FROM player_ratings_current
            WHERE team_code = 'ARG'
              AND (manual_squad_no = 15 OR sportmonks_player_id = 215532
                   OR player_name ILIKE '%GONZALEZ%')
            ORDER BY entity_key
            """
        ).fetchall()
        stale_codes = conn.execute(
            """
            SELECT DISTINCT team_code FROM player_ratings_history
            WHERE team_code NOT IN (SELECT DISTINCT team_code FROM wc2026_squads)
            """
        ).fetchall()
        samples = conn.execute(
            """
            SELECT fixture_id, xg_a, xg_b, prob_a_win, prob_draw, prob_b_win,
                   predicted_at, full_report
            FROM predictions
            ORDER BY predicted_at DESC NULLS LAST
            LIMIT 5
            """
        ).fetchall()
        all_preds = conn.execute(
            """
            SELECT fixture_id, xg_a, xg_b, prob_a_win, prob_draw, prob_b_win, full_report
            FROM predictions
            """
        ).fetchall()

    print(f"\n1. PREDICTIONS: {pred_count} rows")
    print(f"   schema columns: {[c['column_name'] for c in cols]}")

    statuses: Counter[str] = Counter()
    teams_by_status: dict[str, list[str]] = {}

    for row in samples:
        fr = row["full_report"]
        if isinstance(fr, str):
            fr = json.loads(fr)
        xg_a, xg_b = row["xg_a"], row["xg_b"]
        probs = (row["prob_a_win"], row["prob_draw"], row["prob_b_win"])
        pr = fr.get("player_ratings") or {}
        pre43_keys = sorted(k for k in fr.keys() if k != "player_ratings")
        print(f"\n   fixture {row['fixture_id']}: xG={xg_a:.2f}/{xg_b:.2f} "
              f"probs={tuple(round(p, 3) for p in probs)}")
        print(f"     pre-4.3 keys present: {pre43_keys}")
        matchup = fr.get("matchup") or {}
        for side, code_key in (("team_a", "team_a_code"), ("team_b", "team_b_code")):
            block = pr.get(side) or {}
            st = block.get("status", "MISSING")
            tc = matchup.get(code_key) or side
            xi_n = len(block.get("projected_xi") or [])
            bench_n = len(block.get("bench") or [])
            squad_n = len(block.get("squad") or [])
            print(f"     {side} [{tc}]: status={st} xi={xi_n} bench={bench_n} squad={squad_n}")

    for row in all_preds:
        fr = row["full_report"]
        if isinstance(fr, str):
            fr = json.loads(fr)
        pr = fr.get("player_ratings") or {}
        matchup = fr.get("matchup") or {}
        for side, code_key in (("team_a", "team_a_code"), ("team_b", "team_b_code")):
            block = pr.get(side) or {}
            st = block.get("status", "MISSING")
            statuses[st] += 1
            tc = matchup.get(code_key) or side
            teams_by_status.setdefault(st, []).append(str(tc))

    print(f"\n2. DISPLAY status spread (all {len(all_preds)} predictions): {dict(statuses)}")
    for st, teams in sorted(teams_by_status.items()):
        uniq = sorted(set(teams))
        print(f"     {st}: {uniq[:12]}{'...' if len(uniq) > 12 else ''} ({len(uniq)} teams)")

    print(f"\n3. HISTORY today ({today.isoformat()}): {hist_today} rows")
    print("   recent batches:")
    for r in hist_dates:
        print(f"     {r['snapshot_date']}: {r['n']} rows, {r['teams']} teams")
    print("   source breakdown today:")
    for r in source_totals:
        print(f"     {r['source']}: {r['n']} rows, {r['teams']} teams")

    summary = get_player_ratings_snapshot_summary(today)
    distinct_teams = len({r["team_code"] for r in summary["by_team_source"]})
    print(f"   distinct teams (summary): {distinct_teams}")

    print(f"\n4. ARG González #15 override: {[dict(r) for r in arg_gonz]}")
    print(f"   stale non-squad team_codes: {[r['team_code'] for r in stale_codes]}")

    snap_path = Path(__file__).resolve().parent / "_phase43v_xg_snapshot.json"
    core = {
        str(r["fixture_id"]): {
            "xg_a": float(r["xg_a"]),
            "xg_b": float(r["xg_b"]),
            "prob_a_win": float(r["prob_a_win"]),
            "prob_draw": float(r["prob_draw"]),
            "prob_b_win": float(r["prob_b_win"]),
        }
        for r in all_preds
    }
    if label == "BASELINE":
        snap_path.write_text(json.dumps(core, indent=2), encoding="utf-8")
        print(f"\n5. Saved xG snapshot -> {snap_path.name}")
    elif snap_path.exists():
        before = json.loads(snap_path.read_text(encoding="utf-8"))
        mism = [
            fid for fid, vals in before.items()
            if fid not in core or core[fid] != vals
        ]
        if mism:
            print(f"\n5. xG/prob CHANGED vs baseline on fixtures: {mism[:10]}"
                  f"{'...' if len(mism) > 10 else ''} ({len(mism)} total)")
        else:
            print("\n5. xG/prob UNCHANGED vs baseline for all fixtures")

    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "WINDOW":
        _fixture_window_report()
        raise SystemExit(0)
    raise SystemExit(main())
