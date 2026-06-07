"""Smoke test of the full pipeline: bootstrap the pool and predict 3 matchups.

Exercises the uncovered end-to-end path (uncached bootstrap), times it, reports
resolution successes/failures, predicts three deliberately different matchups,
and runs sanity checks on the outputs.

Run from the project root:

    python scripts/predict_real_matchup.py
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline import bootstrap_tournament_pool, predict_matchup  # noqa: E402
from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

MATCHUPS = [
    ("France", "Iran", "favorite vs underdog"),
    ("Argentina", "Brazil", "evenly matched"),
    ("United States", "Spain", "host vs strong non-host"),
]


def _form_count(section: Any) -> int:
    """Count matches whether recent_form is a list or a host placeholder object."""
    if isinstance(section, dict):
        return len(section.get("matches", []))
    return len(section)


def log_rate_limit(label: str) -> None:
    """Best-effort: read the SportMonks rate_limit block off any response."""
    try:
        resp = SportmonksClient().get("leagues", params={"per_page": 1})
        rl = resp.get("rate_limit")
        print(f"  [rate limit @ {label}] {rl}")
    except SportmonksError as exc:
        print(f"  [rate limit @ {label}] unavailable: {exc}")


def main() -> int:
    print("=" * 78)
    print("FULL PIPELINE SMOKE TEST")
    print("=" * 78)

    log_rate_limit("start")

    print("\nBootstrapping tournament pool (force_refresh=True)...")
    t0 = time.perf_counter()
    pool = bootstrap_tournament_pool(force_refresh=True)
    elapsed = time.perf_counter() - t0

    resolved = len(pool.matches_by_team)
    print(f"\nBootstrap took {elapsed:.1f}s")
    print(f"Teams resolved: {resolved} (failed: {len(pool.failed_teams)})")
    if pool.failed_teams:
        print(f"  Failures: {', '.join(pool.failed_teams)}")
    print(f"Baseline: {pool.baseline.baseline_goals_per_match:.3f} goals/match "
          f"({pool.baseline.filtered_match_count} filtered), "
          f"{pool.baseline.baseline_goals_per_team:.3f}/team")

    log_rate_limit("after bootstrap")

    # Hard checks gate the exit code (true pipeline correctness). Data-coverage
    # observations are reported as NOTES, not failures, because they reflect
    # SportMonks plan limitations (no per-match stats for some confederations'
    # qualifiers; hosts play no qualifiers), not a bug in the model code.
    hard: list[tuple[str, bool]] = []
    notes: list[str] = []

    for team_a, team_b, label in MATCHUPS:
        print("\n" + "=" * 78)
        print(f"MATCHUP ({label}): {team_a} vs {team_b}")
        print("=" * 78)
        try:
            report_json = predict_matchup(team_a, team_b, pool)
        except ValueError as exc:
            print(f"  PREDICTION FAILED: {exc}")
            hard.append((f"{team_a} vs {team_b}: prediction produced", False))
            continue
        print(report_json)

        report = json.loads(report_json)
        eg = report["prediction"]["expected_goals"]
        print(f"\n  xG (dampened | raw): {team_a} {eg['team_a']} | {eg['team_a_raw']}   "
              f"{team_b} {eg['team_b']} | {eg['team_b_raw']}   "
              f"(alpha={eg['dampening_alpha']}, ceiling={eg['dampening_ceiling']})")
        if label == "favorite vs underdog":
            print("  [FLAG] Underdog (AFC) has degenerate data — shown to "
                  "illustrate model behavior with poor input data.")
        _sanity_check(report, team_a, team_b, label, hard, notes)

    # -- Recalibration check: Spain vs USA win-probability range -----------
    print("\n" + "=" * 78)
    print("RECALIBRATION CHECK: Spain vs United States")
    print("=" * 78)
    _recalibration_check("Spain", "United States", pool, notes)

    # -- Summary -----------------------------------------------------------
    print("\n" + "=" * 78)
    print("SANITY-CHECK SUMMARY (hard checks gate exit code)")
    print("=" * 78)
    all_ok = True
    for name, ok in hard:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
        all_ok = all_ok and ok

    print("\nDATA-COVERAGE NOTES (informational, not failures):")
    if notes:
        for n in notes:
            print(f"  - {n}")
    else:
        print("  (none)")

    if not all_ok:
        print("\nSome HARD sanity checks FAILED.")
        return 1
    print("\nAll hard sanity checks passed.")
    return 0


def _recalibration_check(
    team_a: str,
    team_b: str,
    pool: Any,
    notes: list[str],
) -> None:
    """Predict team_a (favorite) vs team_b and check the post-fix range.

    After the baseline + goal-unit fixes (v1.1), Spain vs USA should land in a
    defensible band: Spain 55-70%, draw 18-25%, USA 10-22%, with Spain's raw xG
    in ~2.0-3.0 (no longer needing extreme dampening). This is a soft check:
    residual skew is logged as a warning rather than failing the run, since
    calibration is iterative.
    """
    try:
        report = json.loads(predict_matchup(team_a, team_b, pool))
    except ValueError as exc:
        print(f"  RECALIBRATION FAILED: {exc}")
        notes.append(f"{team_a} vs {team_b}: recalibration prediction failed ({exc})")
        return

    probs = report["prediction"]["win_probabilities"]
    eg = report["prediction"]["expected_goals"]
    p_fav = probs["team_a_win"] * 100.0
    p_draw = probs["draw"] * 100.0
    p_dog = probs["team_b_win"] * 100.0
    print(f"  {team_a} (fav) {p_fav:.1f}%  |  draw {p_draw:.1f}%  |  {team_b} {p_dog:.1f}%")
    print(f"  xG (dampened | raw): {team_a} {eg['team_a']} | {eg['team_a_raw']}   "
          f"{team_b} {eg['team_b']} | {eg['team_b_raw']}")

    # Favorite's raw xG should now sit in a realistic band without leaning on
    # the dampening ceiling.
    fav_xg_raw = eg["team_a_raw"]
    if not (2.0 <= fav_xg_raw <= 3.0):
        print(f"  [WARN] {team_a} raw xG {fav_xg_raw} outside expected 2.0-3.0 band.")
        notes.append(f"{team_a} vs {team_b}: {team_a} raw xG {fav_xg_raw} "
                     "outside expected 2.0-3.0 band.")

    in_range = (55.0 <= p_fav <= 70.0 and 18.0 <= p_draw <= 25.0
                and 10.0 <= p_dog <= 22.0)
    if in_range:
        print("  [OK] Win probabilities are within the defensible range.")
    else:
        msg = (f"{team_a} vs {team_b}: win probs ({team_a} {p_fav:.1f}%, draw "
               f"{p_draw:.1f}%, {team_b} {p_dog:.1f}%) outside target band "
               f"(fav 55-70, draw 18-25, dog 10-22) — calibration is iterative; "
               f"a strong team's recent blowout window can still skew this.")
        print(f"  [WARN] {msg}")
        notes.append(msg)


def _sanity_check(
    report: dict[str, Any],
    team_a: str,
    team_b: str,
    label: str,
    hard: list[tuple[str, bool]],
    notes: list[str],
) -> None:
    pred = report["prediction"]
    probs = pred["win_probabilities"]
    p_a, p_d, p_b = probs["team_a_win"], probs["draw"], probs["team_b_win"]
    total = p_a + p_d + p_b
    xg_a = pred["expected_goals"]["team_a"]
    xg_b = pred["expected_goals"]["team_b"]
    form_a = _form_count(report["recent_form"]["team_a"])
    form_b = _form_count(report["recent_form"]["team_b"])
    internals = report["model_internals"]

    def degenerate(side: str) -> bool:
        r = internals[side]
        return r["attack_final"] == 0.0 and r["defense_final"] == 0.0

    def is_host(side: str) -> bool:
        return internals[side].get("data_source") == "synthetic_host_override"

    # A host is "complete" (its synthetic rating is intentional, not a gap).
    a_complete = (is_host("team_a") or form_a == 5) and not degenerate("team_a")
    b_complete = (is_host("team_b") or form_b == 5) and not degenerate("team_b")
    tag = f"{team_a} vs {team_b}"

    # HARD: probabilities must form a valid distribution.
    hard.append((f"{tag}: W/D/L sum ~1.0 (={total:.4f})", abs(total - 1.0) <= 0.01))

    # HARD (only when both teams have real data): favorite must beat underdog.
    if label == "favorite vs underdog":
        if a_complete and b_complete:
            hard.append((f"{tag}: favorite ({team_a}) win% > underdog ({team_b})",
                         p_a > p_b))
        else:
            notes.append(f"{tag}: favorite check SKIPPED (underdog lacks stat data; "
                         f"degenerate rating).")

    # NOTES: data-quality observations.
    if degenerate("team_a"):
        notes.append(f"{team_a}: all-zero rating (no per-match stats in source "
                     f"fixtures -> confederation data gap).")
    if degenerate("team_b"):
        notes.append(f"{team_b}: all-zero rating (no per-match stats in source "
                     f"fixtures -> confederation data gap).")
    if is_host("team_a"):
        notes.append(f"{team_a}: synthetic host rating (data_source="
                     f"synthetic_host_override; no qualifier data).")
    elif form_a != 5:
        notes.append(f"{team_a}: only {form_a} recent match(es).")
    if is_host("team_b"):
        notes.append(f"{team_b}: synthetic host rating (data_source="
                     f"synthetic_host_override; no qualifier data).")
    elif form_b != 5:
        notes.append(f"{team_b}: only {form_b} recent match(es).")
    if not (0.3 <= xg_a <= 4.0):
        notes.append(f"{tag}: {team_a} xG={xg_a} outside heuristic [0.3, 4.0].")
    if not (0.3 <= xg_b <= 4.0):
        notes.append(f"{tag}: {team_b} xG={xg_b} outside heuristic [0.3, 4.0].")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SportmonksError as exc:
        print(f"\n[SPORTMONKS ERROR] {exc}")
        raise SystemExit(1)
