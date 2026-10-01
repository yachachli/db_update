import unittest
import pandas as pd
from cfb.advanced import normalize, features, coverage_report, METRICS


class AdvancedTests(unittest.TestCase):
    def test_coverage_requires_bounded_explicit_override(self):
        games=pd.DataFrame({'game_id':range(200)})
        stats=pd.DataFrame([{'game_id':gid,**{m:1.0 for m in METRICS},'clean_plays':30}
                            for gid in range(199) for _ in range(2)])
        with self.assertRaises(ValueError): coverage_report(games,stats)
        self.assertEqual(coverage_report(games,stats,True)['missing_game_ids'],[199])
        with self.assertRaises(ValueError): coverage_report(games,stats[stats.game_id<190],True)

    def test_clean_efficiency_full_volume(self):
        clean={'offense':{'plays':40,'passingPlays':{'successRate':.4,'explosiveness':1.5},
                          'rushingPlays':{'successRate':.3,'explosiveness':.9}}}
        self.assertEqual(normalize(clean,{'offense':{'plays':70}}),[.4,.3,1.5,.9,40,70])

    def test_invalid_rates_and_missing_values(self):
        with self.assertRaises(ValueError):
            normalize({'offense':{'passingPlays':{'successRate':1.1}}},{'offense':{}})
        self.assertEqual(normalize({'offense':{}},{'offense':{}}),[None]*6)

    def test_no_same_day_or_future_leakage(self):
        games=pd.DataFrame([dict(game_id=i,season=2024,kickoff=pd.Timestamp('2024-09-01',tz='UTC')+pd.Timedelta(days=i//2),
                                 home_id=i%4,away_id=(i+1)%4,neutral_site=False) for i in range(12)])
        stats=pd.DataFrame([dict(game_id=r.game_id,team_id=t,opponent_id=o,pass_success=.4,rush_success=.35,
                                 pass_explosiveness=1.5,rush_explosiveness=.9,clean_plays=50,plays=70)
                            for r in games.itertuples() for t,o in [(r.home_id,r.away_id),(r.away_id,r.home_id)]])
        original=features(games,stats)
        stats.loc[stats.game_id>=10,METRICS]=0
        pd.testing.assert_frame_equal(original,features(games,stats))


if __name__=='__main__': unittest.main()
