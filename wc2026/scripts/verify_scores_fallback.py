"""Quick check: degenerate team count after scores fallback + target fixtures."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.aggregation import compute_team_rating  # noqa: E402
from src.database import get_connection, get_fifa_code_for_team_id, get_wc2026_squad_team_codes  # noqa: E402
from src.pipeline import bootstrap_tournament_pool, predict_matchup_by_id  # noqa: E402

TARGET = (8, 21, 22, 31, 45, 49, 52, 71, 73)


def _usable(m) -> bool:
    return not (
        m.goals_scored == 0 and m.goals_conceded == 0
        and m.xg_created == 0 and m.xg_conceded == 0
    )


def main() -> None:
    print("Re-bootstrapping pool with scores fallback...")
    pool = bootstrap_tournament_pool(force_refresh=True)
    squad = set(get_wc2026_squad_team_codes())

    degen: list[tuple] = []
    ok: list[tuple] = []
    for tid in sorted(pool.matches_by_team):
        code = get_fifa_code_for_team_id(tid) or ""
        if code not in squad:
            continue
        team = pool.teams[tid]
        matches = pool.matches_by_team[tid]
        if not matches:
            degen.append((code, team.name, 0, 0))
            continue
        usable = sum(1 for m in matches if _usable(m))
        r = compute_team_rating(team, matches, pool.baseline)
        if r.attack_final == 0.0 and r.defense_final == 0.0:
            degen.append((code, team.name, usable, len(matches)))
        else:
            ok.append((code, round(r.attack_final, 3), round(r.defense_final, 3), usable))

    print(f"Non-degenerate squad teams: {len(ok)}")
    print(f"Still degenerate: {len(degen)}")
    if degen:
        for row in degen:
            print(f"  {row}")
    print("Sample ratings:", ok[:10])

    print("\nTarget fixtures:")
    for fid in TARGET:
        with get_connection() as conn:
            fx = dict(conn.execute(
                "SELECT team_a_id, team_b_id, team_a_name, team_b_name "
                "FROM wc2026_fixtures WHERE fixture_id=%s", (fid,)
            ).fetchone())
        rep = json.loads(predict_matchup_by_id(
            int(fx["team_a_id"]), int(fx["team_b_id"]), pool
        ))
        xg = rep["prediction"]["expected_goals"]
        pr = rep["prediction"]["win_probabilities"]
        deg = xg["team_a"] == 0 and xg["team_b"] == 0
        print(
            f"  F{fid} {fx['team_a_name']} vs {fx['team_b_name']}: "
            f"xG={xg['team_a']}/{xg['team_b']} "
            f"probs=({pr['team_a_win']:.3f},{pr['draw']:.3f},{pr['team_b_win']:.3f}) "
            f"degenerate={deg}"
        )


if __name__ == "__main__":
    main()
