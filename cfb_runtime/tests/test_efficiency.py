import unittest
import numpy as np
import pandas as pd
from cfb.efficiency import fit_ratings, efficiency_features, bracket
from cfb.team_stats import normalize


class EfficiencyTests(unittest.TestCase):
    def test_missing_is_not_zero_and_negative_yards_valid(self):
        self.assertEqual(normalize({'stats':[]}), (None,None,None,None))
        self.assertEqual(normalize({'stats':[
            {'category':'completionAttempts','stat':'0-0'},
            {'category':'rushingYards','stat':'-12'},
            {'category':'rushingAttempts','stat':'4'}]}),(None,0,-12,4))
        with self.assertRaises(ValueError):
            normalize({'stats':[{'category':'completionAttempts','stat':'8-4'}]})

    def test_joint_ratings_recover_offense_and_defense_order(self):
        rows=[]
        for _ in range(20):
            for team in range(4):
                for opponent in range(4):
                    if team==opponent: continue
                    rows.append(dict(team_id=team,opponent_id=opponent,season=2024,home_field=0,
                                     pass_attempts=30,pass_yards=30*(6+team*.5-opponent*.4)))
        fit,_=fit_ratings(pd.DataFrame(rows),'pass',2024)
        self.assertGreater(fit[3]['offense'],fit[0]['offense'])
        self.assertLess(fit[3]['allowance'],fit[0]['allowance'])

    def test_same_day_and_future_box_scores_do_not_leak(self):
        games=pd.DataFrame([dict(game_id=i,season=2024,kickoff=pd.Timestamp('2024-09-01',tz='UTC')+pd.Timedelta(days=i//2),
                                 home_id=i%4,away_id=(i+1)%4,neutral_site=False) for i in range(12)])
        stats=pd.DataFrame([dict(game_id=r.game_id,team_id=t,opponent_id=o,pass_yards=200+r.game_id,
                                  pass_attempts=30,rush_yards=130,rush_attempts=35)
                            for r in games.itertuples() for t,o in [(r.home_id,r.away_id),(r.away_id,r.home_id)]])
        before,_=efficiency_features(games,stats)
        targeted,_=efficiency_features(games,stats,prediction_ids={10})
        pd.testing.assert_frame_equal(before[before.game_id==10].reset_index(drop=True),targeted.reset_index(drop=True))
        stats.loc[stats.game_id>=10,'pass_yards']=2000
        after,_=efficiency_features(games,stats)
        pd.testing.assert_frame_equal(before,after)
        self.assertTrue(np.isfinite(before.to_numpy()).all())

    def test_zero_attempts_ignored(self):
        data=pd.DataFrame([dict(team_id=1,opponent_id=2,pass_yards=0,pass_attempts=0,season=2024,home_field=0)])
        self.assertEqual(fit_ratings(data,'pass',2024),({},0.0))

    def test_brackets(self):
        self.assertEqual(bracket(.95),'top_decile')
        self.assertEqual(bracket(.05),'bottom_decile')


if __name__=='__main__': unittest.main()
