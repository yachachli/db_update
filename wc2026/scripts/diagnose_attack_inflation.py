"""Diagnose why a team's ``attack_final`` is unrealistically large.

Spain's ``attack_final`` lands around ~6.9 when a top attack should sit in the
~1.3-1.6 range. This script walks the aggregation pipeline one stage at a time
for Spain (and a strong reference nation) and flags the exact step where the
numbers leave a believable range.

It is read-only: it imports the production functions and re-runs them on the
cached tournament pool, but does NOT modify any production code or data.

Run from the project root:

    python scripts/diagnose_attack_inflation.py
    python scripts/diagnose_attack_inflation.py --force-refresh
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from statistics import mean
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.aggregation import (  # noqa: E402
    compute_match_weights,
    compute_raw_attack_rating,
    compute_team_rating,
    compute_weighted_offensive_stats,
)
from src.config import (  # noqa: E402
    CONFEDERATION_MULTIPLIERS,
    HOST_BONUS,
    OFFENSIVE_STAT_WEIGHTS,
)
from src.math_utils import (  # noqa: E402
    match_quality_weight,
    opponent_strength,
    venue_multiplier,
)
from src.models import MatchStats, Team, TournamentBaseline  # noqa: E402
from src.pipeline import bootstrap_tournament_pool  # noqa: E402

_OFFENSIVE_STATS = tuple(OFFENSIVE_STAT_WEIGHTS)

# Rough "this looks like real soccer" expectations, used only for the PASS/FAIL
# annotations in the diagnosis block. They are deliberately generous.
_EXPECTED = {
    "phase2_weight": (0.5, 1.5),
    "phase4_attack_raw": (1.0, 2.5),
    "phase5_normalized": (0.7, 1.6),
    "phase6_final": (0.7, 1.7),
}


def _find_team(pool: Any, *names: str) -> tuple[int, Team] | tuple[None, None]:
    """Return (team_id, Team) for the first matching name (case-insensitive)."""
    wanted = {n.lower() for n in names}
    for team_id, team in pool.teams.items():
        if team.name.lower() in wanted:
            return team_id, team
    return None, None


def _check(value: float, low: float, high: float) -> str:
    return "OK" if low <= value <= high else "OUT OF RANGE"


# ---------------------------------------------------------------------------
# Per-team walkthrough
# ---------------------------------------------------------------------------
def diagnose_team(
    label: str,
    team: Team,
    matches: list[MatchStats],
    baseline: TournamentBaseline,
    *,
    verbose: bool = True,
) -> dict[str, Any]:
    """Walk one team through phases 1-6 and return the computed checkpoints."""
    bar = "=" * 78
    if verbose:
        print(f"\n{bar}\n{label}: {team.name}\n{bar}")

    # -- PHASE 1: raw match data -----------------------------------------
    if verbose:
        print("\n--- PHASE 1: raw match data ---")
        print(f"  Team: id={team.team_id} name={team.name!r} "
              f"fifa_points={team.fifa_points} conf={team.confederation} "
              f"is_host={team.is_host}")
        print(f"  {len(matches)} matches in window:")
        for m in matches:
            print(f"    {m.date:%Y-%m-%d}  vs {m.opponent_name or m.opponent_id:<20} "
                  f"[{m.venue:<7}]  score {m.goals_scored}-{m.goals_conceded}")
            print(f"        xg_created={m.xg_created:.2f}  "
                  f"big_chances_created={m.big_chances_created}  "
                  f"shots_on_target={m.shots_on_target}  "
                  f"xgot_created={m.xgot_created:.2f}  "
                  f"goals_scored={m.goals_scored}  "
                  f"opp_fifa_pts={m.opponent_fifa_points:.0f}")

    # -- PHASE 2: match quality weights ----------------------------------
    weights = compute_match_weights(matches)
    if verbose:
        print("\n--- PHASE 2: match quality weights ---")
        for m, w in zip(matches, weights):
            os_ = opponent_strength(m.opponent_fifa_points)
            vm = venue_multiplier(m.venue)
            print(f"    vs {m.opponent_name or m.opponent_id:<20} "
                  f"opp_strength={os_:.3f}  venue_mult={vm:.3f}  "
                  f"=> weight={w:.3f}")
        print(f"  sum(weights) = {sum(weights):.3f}")

    # -- PHASE 3: offensive stat aggregation, step by step ---------------
    offensive_stats = compute_weighted_offensive_stats(matches, weights)
    weight_total = sum(weights)
    if verbose:
        print("\n--- PHASE 3: offensive stat aggregation ---")
        for stat in _OFFENSIVE_STATS:
            raw_vals = [float(getattr(m, stat)) for m in matches]
            wsum = sum(v * w for v, w in zip(raw_vals, weights))
            wavg = offensive_stats[stat]
            unweighted = mean(raw_vals) if raw_vals else 0.0
            flag = ""
            # Weighted avg should sit near the unweighted mean; flag big drift.
            if unweighted > 0 and abs(wavg - unweighted) / unweighted > 0.5:
                flag = "  <-- FLAG: weighted avg far from raw mean"
            print(f"    {stat}:")
            print(f"        raw     = {[round(v, 2) for v in raw_vals]}")
            print(f"        wsum    = {wsum:.3f}   normalizer = {weight_total:.3f}")
            print(f"        wavg    = {wavg:.3f}   (unweighted mean = {unweighted:.3f}){flag}")

    # -- PHASE 4: raw attack rating --------------------------------------
    attack_raw = compute_raw_attack_rating(offensive_stats)
    if verbose:
        print("\n--- PHASE 4: raw attack rating ---")
        print(f"  OFFENSIVE_STAT_WEIGHTS = {dict(OFFENSIVE_STAT_WEIGHTS)}")
        for stat, w in OFFENSIVE_STAT_WEIGHTS.items():
            print(f"    {stat}: wavg={offensive_stats[stat]:.3f} x cfg_weight={w} "
                  f"= {offensive_stats[stat] * w:.3f}")
        print(f"  attack_raw = {attack_raw:.3f}  "
              f"[{_check(attack_raw, *_EXPECTED['phase4_attack_raw'])}; "
              f"expected ~1.0-2.5]")

    # -- PHASE 5: normalization ------------------------------------------
    per_team = baseline.baseline_goals_per_team
    attack_normalized = attack_raw / per_team if per_team else 0.0
    if verbose:
        print("\n--- PHASE 5: normalization ---")
        print(f"  baseline_goals_per_match = {baseline.baseline_goals_per_match:.3f}")
        print(f"  baseline_goals_per_team  = {per_team:.3f}")
        print(f"  filtered_match_count     = {baseline.filtered_match_count}")
        print(f"  attack_normalized = attack_raw / baseline_goals_per_team "
              f"= {attack_raw:.3f} / {per_team:.3f} = {attack_normalized:.3f}  "
              f"[{_check(attack_normalized, *_EXPECTED['phase5_normalized'])}; "
              f"expected ~0.7-1.6]")

    # -- PHASE 6: final adjustments --------------------------------------
    conf_mult = CONFEDERATION_MULTIPLIERS.get(team.confederation, 1.0)
    host_mult = HOST_BONUS if team.is_host else 1.0
    attack_final = attack_normalized * conf_mult * host_mult
    if verbose:
        print("\n--- PHASE 6: final adjustments ---")
        print(f"  confederation = {team.confederation}  "
              f"CONFEDERATION_MULTIPLIERS = {conf_mult}")
        print(f"  HOST_BONUS applied = {host_mult} (is_host={team.is_host})")
        print(f"  attack_final = {attack_normalized:.3f} x {conf_mult} x {host_mult} "
              f"= {attack_final:.3f}  "
              f"[{_check(attack_final, *_EXPECTED['phase6_final'])}; "
              f"expected ~0.7-1.7]")

    # Cross-check against the production pipeline.
    rating = compute_team_rating(team, matches, baseline)
    if verbose and abs(rating.attack_final - attack_final) > 1e-6:
        print(f"  NOTE: production compute_team_rating gives "
              f"attack_final={rating.attack_final:.3f} (manual={attack_final:.3f})")

    return {
        "weights": weights,
        "offensive_stats": offensive_stats,
        "attack_raw": attack_raw,
        "attack_normalized": attack_normalized,
        "attack_final": attack_final,
        "matches": matches,
    }


def _diagnosis_block(team: Team, cp: dict[str, Any], baseline: TournamentBaseline) -> None:
    """Print the final PASS/FAIL diagnosis for one team."""
    weights = cp["weights"]
    offensive_stats = cp["offensive_stats"]

    # Phase 1: do raw stats look like real soccer (not all-zero, not absurd)?
    avg_xg = mean(float(getattr(m, "xg_created")) for m in cp["matches"])
    avg_goals = mean(float(getattr(m, "goals_scored")) for m in cp["matches"])
    phase1_ok = (avg_xg + avg_goals) > 0 and avg_xg < 6 and avg_goals < 6

    # Phase 2: all weights in the 0.5-1.5 band?
    lo, hi = _EXPECTED["phase2_weight"]
    phase2_ok = all(lo <= w <= hi for w in weights)

    # Phase 3: every weighted avg near its unweighted mean?
    phase3_ok = True
    for stat in _OFFENSIVE_STATS:
        raw_vals = [float(getattr(m, stat)) for m in cp["matches"]]
        unweighted = mean(raw_vals) if raw_vals else 0.0
        if unweighted > 0 and abs(offensive_stats[stat] - unweighted) / unweighted > 0.5:
            phase3_ok = False

    phase4_ok = _EXPECTED["phase4_attack_raw"][0] <= cp["attack_raw"] <= _EXPECTED["phase4_attack_raw"][1]
    phase5_ok = _EXPECTED["phase5_normalized"][0] <= cp["attack_normalized"] <= _EXPECTED["phase5_normalized"][1]
    phase6_ok = _EXPECTED["phase6_final"][0] <= cp["attack_final"] <= _EXPECTED["phase6_final"][1]

    def mark(ok: bool) -> str:
        return "OK " if ok else "BUG"

    print("\n" + "=" * 78)
    print(f"DIAGNOSIS ({team.name})")
    print("=" * 78)
    print("Step where values exit reasonable range:")
    print(f"  Phase 1 (raw stats):       [{mark(phase1_ok)}] values look like real soccer stats?")
    print(f"  Phase 2 (weights):         [{mark(phase2_ok)}] weights in 0.5-1.5 range?")
    print(f"  Phase 3 (weighted stats):  [{mark(phase3_ok)}] weighted averages near raw averages?")
    print(f"  Phase 4 (raw attack):      [{mark(phase4_ok)}] combined rating in 1.0-2.5 range?")
    print(f"  Phase 5 (normalization):   [{mark(phase5_ok)}] normalized in 0.7-1.6 range?")
    print(f"  Phase 6 (final adjustment):[{mark(phase6_ok)}] final in 0.7-1.7 range?")
    print()
    print("The OK marks indicate where the bug ISN'T. The first BUG shows where it IS.")

    # Pinpoint the first failing phase.
    phases = [
        ("Phase 1 (raw stats)", phase1_ok),
        ("Phase 2 (weights)", phase2_ok),
        ("Phase 3 (weighted stats)", phase3_ok),
        ("Phase 4 (raw attack)", phase4_ok),
        ("Phase 5 (normalization)", phase5_ok),
        ("Phase 6 (final adjustment)", phase6_ok),
    ]
    first_bad = next((name for name, ok in phases if not ok), None)
    if first_bad:
        print(f"\n  => First step out of range: {first_bad}")
    else:
        print("\n  => All phases in range for this team.")

    # Baseline-specific warning (the prime suspect).
    per_team = baseline.baseline_goals_per_team
    if per_team < 1.0 or per_team > 2.0:
        print("\n  WARNING: baseline_goals_per_team = "
              f"{per_team:.3f} is outside the plausible ~1.1-1.5 band.")
        print(f"           filtered_match_count = {baseline.filtered_match_count} "
              f"(threshold = {baseline.fifa_points_threshold:.0f} FIFA pts).")
        print("           A low per-team baseline divides every attack_raw up, "
              "inflating attack_normalized and thus attack_final.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force-refresh", action="store_true",
                        help="Rebuild the tournament pool instead of using cache.")
    args = parser.parse_args()

    print("Loading tournament pool"
          + (" (force_refresh=True)" if args.force_refresh else " (cached if fresh)")
          + "...")
    pool = bootstrap_tournament_pool(force_refresh=args.force_refresh)
    baseline = pool.baseline
    print(f"Pool: {len(pool.teams)} teams, {len(pool.all_matches)} matches, "
          f"baseline_goals_per_team={baseline.baseline_goals_per_team:.3f}, "
          f"filtered_match_count={baseline.filtered_match_count}")

    # -- Subject: Spain --------------------------------------------------
    spain_id, spain = _find_team(pool, "Spain")
    if spain is None:
        print("\nERROR: Spain not found in pool.teams.")
        print("Available teams:", ", ".join(sorted(t.name for t in pool.teams.values())))
        return 1

    spain_matches = pool.matches_by_team.get(spain_id, [])
    if not spain_matches:
        print(f"\nERROR: no matches cached for Spain (id={spain_id}). "
              "Spain may be a host or failed to fetch.")
        return 1

    spain_cp = diagnose_team("SUBJECT", spain, spain_matches, baseline)

    # -- PHASE 7: ground-truth reference (Brazil, falling back to Argentina)
    print("\n" + "#" * 78)
    print("PHASE 7: ground-truth reference comparison")
    print("#" * 78)
    ref_id, ref = _find_team(pool, "Brazil", "Argentina")
    ref_cp = None
    if ref is None or not pool.matches_by_team.get(ref_id):
        print("  Reference team (Brazil/Argentina) unavailable; skipping comparison.")
    else:
        ref_cp = diagnose_team("REFERENCE", ref, pool.matches_by_team[ref_id], baseline)
        print("\n--- Spain vs reference attack_final ---")
        print(f"  {spain.name:<12} attack_final = {spain_cp['attack_final']:.3f}")
        print(f"  {ref.name:<12} attack_final = {ref_cp['attack_final']:.3f}")

    # -- PHASE 8: diagnosis ----------------------------------------------
    _diagnosis_block(spain, spain_cp, baseline)
    if ref_cp is not None:
        _diagnosis_block(ref, ref_cp, baseline)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
