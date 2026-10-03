"""Regressions for failure modes that could wedge or crash the deployed runtime."""
from datetime import datetime, timezone
import unittest
from unittest.mock import patch
import numpy as np
import pandas as pd
from cfb.backtest import rating_features
from cfb.efficiency import efficiency_features
from cfb.pipeline import coverage, repair

KICK = datetime(2026, 9, 5, tzinfo=timezone.utc)
STATS = ['game_id', 'team_id', 'opponent_id', 'pass_yards', 'pass_attempts', 'rush_yards', 'rush_attempts']


def schedule(count, week, kind='regular', start=0):
    return [(start + i, week, kind, KICK.replace(day=5 + week)) for i in range(count)]


class CoverageGateTests(unittest.TestCase):
    """A bounded refresh must not block forever on gaps it cannot reach."""

    def gate(self, incomplete, lookback=3):
        rows = schedule(2, 1) + schedule(2, 2, start=10) + schedule(2, 3, start=20) + schedule(2, 4, start=30)
        with patch('cfb.pipeline.connect') as connect:
            conn = connect.return_value.__enter__.return_value
            conn.execute.return_value.fetchall.side_effect = [
                [(2024, 700), (2025, 700)], rows, rows, [(g,) for g in incomplete]]
            return coverage(2026, lookback)

    def test_gap_inside_refresh_window_still_blocks(self):
        # Weeks 2-4 are refreshed every run, so a gap there is a genuine failure.
        with self.assertRaisesRegex(ValueError, 'inside the refresh window'):
            self.gate([30])

    def test_gap_outside_window_reports_backlog_without_wedging(self):
        # Week 1 is unreachable by a 3-partition refresh; blocking would never clear.
        report = self.gate([0, 1])
        self.assertEqual(report['incomplete_game_ids'], [])
        self.assertEqual(report['stale_backlog_game_ids'], [0, 1])
        self.assertEqual(report['stale_backlog_partitions'], [[1, 'regular']])

    def test_backlog_and_window_gaps_are_reported_separately(self):
        with self.assertRaises(ValueError):
            self.gate([0, 30])

    def test_clean_season_reports_no_work(self):
        report = self.gate([])
        self.assertEqual(report['stale_backlog_game_ids'], [])
        self.assertEqual(report['completed_games_checked'], 8)


class RepairTests(unittest.TestCase):
    def test_repair_is_bounded_and_reports_remaining(self):
        with patch('cfb.team_stats.ingest') as team, patch('cfb.players.ingest') as players:
            result = repair(2026, [[1, 'regular'], [2, 'regular'], [3, 'regular']], limit=2)
        self.assertEqual(result['repaired_partitions'], [[1, 'regular'], [2, 'regular']])
        self.assertEqual(result['remaining_partitions'], 1)
        team.assert_called_once()
        players.assert_called_once()
        self.assertEqual(team.call_args.kwargs['partitions'], [(1, 'regular'), (2, 'regular')])

    def test_no_backlog_does_no_ingestion(self):
        with patch('cfb.team_stats.ingest') as team, patch('cfb.players.ingest') as players:
            result = repair(2026, [])
        team.assert_not_called()
        players.assert_not_called()
        self.assertEqual(result['remaining_partitions'], 0)


class ColdStartTests(unittest.TestCase):
    """Too little history must reach the insufficient-history path, not raise KeyError."""

    def test_empty_rating_features_keep_merge_contract(self):
        games = pd.DataFrame([{'game_id': 1, 'season': 2026, 'kickoff': pd.Timestamp(KICK),
                               'home_id': 10, 'away_id': 20, 'neutral_site': False,
                               'home_points': np.nan, 'away_points': np.nan}])
        ratings = rating_features(games, prediction_ids={1})
        self.assertEqual(len(ratings), 0)
        self.assertIn('game_id', ratings.columns)
        efficiency = efficiency_features(games, pd.DataFrame(columns=STATS), prediction_ids={1})[0]
        context = ratings.merge(efficiency, on='game_id', validate='one_to_one')
        self.assertEqual(len(context), 0)


if __name__ == '__main__':
    unittest.main()
