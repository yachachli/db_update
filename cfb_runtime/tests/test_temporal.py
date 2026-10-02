import unittest
import numpy as np
import pandas as pd
from cfb.backtest import (HALF_LIFE_DAYS, MARGIN_ALPHA, RATING_COLUMNS, TOTAL_ALPHA,
                          rating_features)


def season_of_games(season, start, count=120, teams=6):
    rows = []
    for i in range(count):
        rows.append(dict(game_id=season * 1000 + i, season=season,
                         kickoff=pd.Timestamp(start, tz='UTC') + pd.Timedelta(days=i // 2),
                         home_id=i % teams, away_id=(i + 1) % teams, neutral_site=False,
                         home_points=24 + i % 10, away_points=17 + i % 7))
    return pd.DataFrame(rows)


class TemporalTests(unittest.TestCase):
    def test_future_scores_cannot_change_earlier_features(self):
        rows=[]
        for i in range(110):
            rows.append(dict(game_id=i,season=2024,kickoff=pd.Timestamp('2024-01-01',tz='UTC')+pd.Timedelta(days=i//2),home_id=i%4,away_id=(i+1)%4,neutral_site=False,home_points=24+i%10,away_points=17+i%7))
        games=pd.DataFrame(rows)
        original=rating_features(games)
        targeted=rating_features(games,prediction_ids={108})
        pd.testing.assert_frame_equal(original[original.game_id==108].reset_index(drop=True),targeted.reset_index(drop=True))
        changed=games.copy()
        changed.loc[changed.game_id>=108,'home_points']=99
        after=rating_features(changed)
        cols=['game_id','margin_rating','total_rating']
        pd.testing.assert_frame_equal(original[cols],after[cols])
        self.assertTrue(len(original)>0)


class RatingConfigurationTests(unittest.TestCase):
    def test_margin_and_total_use_separate_regularization(self):
        # A shared alpha cannot suit both; margin is a difference, total a sum.
        self.assertLess(MARGIN_ALPHA, TOTAL_ALPHA)

    def test_recency_decays_within_a_season(self):
        # The previous season-step decay weighted week 1 and week 13 identically.
        games = season_of_games(2024, '2024-01-01')
        early = games.copy()
        late = games.copy()
        # Move the same scoreline from the start of the window to just before the
        # prediction date; a day-level decay must notice, a season step cannot.
        early.loc[early.index[:6], 'home_points'] = 70
        late.loc[late.index[100:106], 'home_points'] = 70
        a = rating_features(early).set_index('game_id').margin_rating
        b = rating_features(late).set_index('game_id').margin_rating
        shared = a.index.intersection(b.index)
        self.assertTrue((a[shared] - b[shared]).abs().max() > 1e-6)
        self.assertGreater(HALF_LIFE_DAYS, 0)

    def test_empty_history_returns_the_shaped_frame(self):
        games = season_of_games(2024, '2024-01-01', count=4)
        result = rating_features(games)
        self.assertEqual(len(result), 0)
        self.assertEqual(list(result.columns), RATING_COLUMNS)

    def test_lookback_window_excludes_distant_seasons(self):
        old = season_of_games(2019, '2019-01-01')
        new = season_of_games(2024, '2024-01-01')
        with_old = rating_features(pd.concat([old, new], ignore_index=True))
        without = rating_features(new)
        cols = ['game_id', 'margin_rating', 'total_rating']
        merged = with_old[cols].merge(without[cols], on='game_id', suffixes=('_a', '_b'))
        self.assertTrue(len(merged) > 0)
        np.testing.assert_allclose(merged.margin_rating_a, merged.margin_rating_b, atol=1e-9)


if __name__=='__main__': unittest.main()
