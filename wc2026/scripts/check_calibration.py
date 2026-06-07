"""Print calibration targets from the cached tournament pool."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.pipeline import bootstrap_tournament_pool, predict_matchup_by_id


def main() -> None:
    pool = bootstrap_tournament_pool()
    targets = [
        (18706, 18704, "SCO-BRA"),
        (18559, 18644, "JOR-ARG"),
    ]
    for team_a_id, team_b_id, label in targets:
        report = json.loads(predict_matchup_by_id(team_a_id, team_b_id, pool))
        pred = report["prediction"]
        print(label, report["matchup"])
        print("  xG", pred["expected_goals"])
        print("  probs", pred["win_probabilities"])
        for side in ("team_a", "team_b"):
            rating = report["model_internals"][side]
            print(
                f"  {side} atk={rating['attack_final']:.3f} "
                f"def={rating['defense_final']:.3f}"
            )
        print()


if __name__ == "__main__":
    main()
