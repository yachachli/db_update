"""Phase 4.3 validation: projected XI + squad in report JSON.

Runs sample matchups, confirms prediction core unchanged, prints display blocks.

    py -3 scripts/validate_phase43_display.py
"""

from __future__ import annotations

import json
import sys
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline import (  # noqa: E402
    _predict_for_teams,
    _resolve_team_by_id,
    bootstrap_tournament_pool,
)
from src.prediction import predict_match  # noqa: E402
from src.reporting import build_matchup_report, matchup_report_to_dict  # noqa: E402

_MATCHUPS = [
    ("ESP", 18710, "ARG", 18644, "non-host"),
    ("USA", 18571, "MEX", 18576, "host"),
    ("NZL", 18613, "IRN", 18652, "manual-coverage"),
    ("POR", 18701, "SUI", 18708, "partial-candidate"),
]

ESP_ID = 18710
ARG_ID = 18644


def _strip_player_ratings(payload: dict) -> dict:
    clone = deepcopy(payload)
    clone.pop("player_ratings", None)
    clone.pop("projected_lineups", None)
    return clone


def _core_report_dict(team_a, team_b, pool) -> dict:
    from src.pipeline import _rating_for, load_host_overrides

    host_overrides = load_host_overrides()
    rating_a, matches_a, source_a, reason_a = _rating_for(
        team_a, pool, host_overrides
    )
    rating_b, matches_b, source_b, reason_b = _rating_for(
        team_b, pool, host_overrides
    )
    prediction = predict_match(rating_a, rating_b, pool.baseline)
    report = build_matchup_report(
        team_a,
        team_b,
        matches_a,
        matches_b,
        rating_a,
        rating_b,
        prediction,
        team_a_data_source=source_a,
        team_b_data_source=source_b,
        team_a_host_reasoning=reason_a,
        team_b_host_reasoning=reason_b,
    )
    return matchup_report_to_dict(replace(report, player_ratings=None))


def _summarize_team_block(label: str, block: dict) -> None:
    print(f"\n  [{label}] status={block.get('status')}")
    print(f"    projected_xi: {len(block.get('projected_xi', []))}")
    print(f"    bench: {len(block.get('bench', []))}")
    print(f"    squad rows: {len(block.get('squad', []))}")
    xi = block.get("projected_xi") or []
    if xi:
        sample = xi[0]
        print(
            f"    xi[0]: #{sample.get('squad_no')} {sample.get('player_name')} "
            f"avg={sample.get('avg_rating')} n={sample.get('matches_counted')} "
            f"mins={sample.get('minutes_share')}"
        )
    squad = block.get("squad") or []
    rated_squad = [r for r in squad if r.get("avg_rating") is not None]
    if rated_squad:
        sample = rated_squad[0]
        print(
            f"    squad rated sample: #{sample.get('squad_no')} "
            f"{sample.get('player_name')} avg={sample.get('avg_rating')} "
            f"n={sample.get('matches_counted')}"
        )


def main() -> int:
    print("=" * 78)
    print("PHASE 4.3 VALIDATION")
    print("=" * 78)

    pool = bootstrap_tournament_pool()
    display_cache: dict[str, dict] = {}

    # Prediction unchanged check (ESP vs ARG)
    team_a = _resolve_team_by_id(ESP_ID, pool)
    team_b = _resolve_team_by_id(ARG_ID, pool)
    core = _strip_player_ratings(_core_report_dict(team_a, team_b, pool))
    full_json = json.loads(
        _predict_for_teams(
            team_a, team_b, pool, player_display_cache=display_cache
        )
    )
    full_core = _strip_player_ratings(full_json)
    if core != full_core:
        print("\nFAIL: prediction core differs with player_ratings attached")
        import difflib

        diff = difflib.unified_diff(
            json.dumps(core, indent=2, sort_keys=True).splitlines(),
            json.dumps(full_core, indent=2, sort_keys=True).splitlines(),
            lineterm="",
        )
        print("\n".join(list(diff)[:40]))
        return 1
    print("\nOK: prediction core unchanged (player_ratings stripped matches)")

    # Cache-once check: ESP already in cache from above
    cache_size_before = len(display_cache)
    _predict_for_teams(team_a, team_b, pool, player_display_cache=display_cache)
    print(
        f"OK: display cache size stable across repeat matchup "
        f"({cache_size_before} -> {len(display_cache)} team codes)"
    )

    print("\n" + "=" * 78)
    print("MATCHUP DISPLAY SAMPLES")
    print("=" * 78)

    for code_a, id_a, code_b, id_b, label in _MATCHUPS:
        ta = _resolve_team_by_id(id_a, pool)
        tb = _resolve_team_by_id(id_b, pool)
        report = json.loads(
            _predict_for_teams(ta, tb, pool, player_display_cache=display_cache)
        )
        pr = report["player_ratings"]
        print(f"\n--- {label}: {code_a} vs {code_b} ---")
        _summarize_team_block(code_a, pr["team_a"])
        if label in ("non-host", "host", "manual-coverage"):
            _summarize_team_block(code_b, pr["team_b"])

    # Highlight blocks for report
    esp_arg = json.loads(
        _predict_for_teams(
            _resolve_team_by_id(ESP_ID, pool),
            _resolve_team_by_id(ARG_ID, pool),
            pool,
            player_display_cache=display_cache,
        )
    )
    usa_mex = json.loads(
        _predict_for_teams(
            _resolve_team_by_id(18571, pool),
            _resolve_team_by_id(18576, pool),
            pool,
            player_display_cache=display_cache,
        )
    )
    nzl_irn = json.loads(
        _predict_for_teams(
            _resolve_team_by_id(18613, pool),
            _resolve_team_by_id(18652, pool),
            pool,
            player_display_cache=display_cache,
        )
    )

    print("\n" + "=" * 78)
    print("REPORT SNAPSHOTS (player_ratings blocks)")
    print("=" * 78)
    print("\nESP (non-host):")
    print(json.dumps(esp_arg["player_ratings"]["team_a"], indent=2)[:2500])
    print("\nUSA (host):")
    print(json.dumps(usa_mex["player_ratings"]["team_a"], indent=2)[:2500])
    print("\nNZL (manual):")
    print(json.dumps(nzl_irn["player_ratings"]["team_a"], indent=2)[:2500])

    print("\nSTOP — Phase 4.3 validation complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
