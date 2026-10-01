import unittest
import pandas as pd
from cfb.backtest import rating_features


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


if __name__=='__main__': unittest.main()
