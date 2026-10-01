import unittest
import numpy as np
import pandas as pd
from cfb.players import parse
from cfb.pou import candidates, fit_predict, predict_snapshot, COLS


def fixture():
    games=pd.DataFrame([dict(game_id=i,season=2024,kickoff=pd.Timestamp('2024-09-01',tz='UTC')+pd.Timedelta(days=7*i),
                             home_id=1,away_id=2) for i in range(6)])
    observations=pd.DataFrame([dict(game_id=i,player_id='p1',team_id=1,player_name='Test',category='rushing',volume=10,yards=50+i)
                               for i in range(6)])
    context=pd.DataFrame([dict(game_id=i,margin_rating=3,total_rating=50,home_pass_allowance=0,away_pass_allowance=0,
                               home_rush_allowance=0,away_rush_allowance=0) for i in range(6)])
    return games,observations,context


class POUTests(unittest.TestCase):
    def test_saved_model_reproduces_predictions(self):
        rng=np.random.default_rng(7)
        train=pd.DataFrame(rng.normal(size=(150,len(COLS))),columns=COLS)
        train['actual_volume']=rng.integers(1,20,size=150)
        train['actual_yards']=train.actual_volume*5+rng.normal(size=150)
        test=train.iloc[:10][COLS].copy()
        volume,yards,bundle=fit_predict(train,test)
        saved_volume,saved_yards=predict_snapshot(bundle,test)
        np.testing.assert_allclose(saved_volume,volume)
        np.testing.assert_allclose(saved_yards,yards)

    def test_parser_preserves_negative_yards_and_missing(self):
        self.assertEqual(parse('passing',{'C/ATT':'3/5','YDS':'12'}),(5,12))
        self.assertEqual(parse('rushing',{'CAR':'2','YDS':'-5'}),(2,-5))
        self.assertIsNone(parse('receiving',{'REC':'1'}))
        with self.assertRaises(ValueError): parse('passing',{'C/ATT':'5/3','YDS':'12'})

    def test_current_outcome_cannot_change_eligibility_or_features(self):
        games,obs,ctx=fixture()
        before=candidates(games,obs,ctx)
        obs.loc[obs.game_id==5,['yards','volume']]=[999,90]
        after=candidates(games,obs,ctx)
        pd.testing.assert_frame_equal(before[['game_id',*COLS]],after[['game_id',*COLS]])
        self.assertEqual(before.game_id.tolist(),[3,4,5])

    def test_absence_stays_candidate_but_not_zero(self):
        games,obs,ctx=fixture()
        result=candidates(games,obs[obs.game_id!=5],ctx)
        row=result[result.game_id==5].iloc[0]
        self.assertTrue(pd.isna(row.actual_yards))
        self.assertTrue(pd.isna(row.actual_volume))

    def test_season_and_team_reset_history(self):
        games,obs,ctx=fixture()
        games.loc[games.game_id>=3,'season']=2025
        self.assertTrue(candidates(games,obs,ctx).empty)
        games,obs,ctx=fixture()
        games.loc[games.game_id>=3,'home_id']=3
        obs.loc[obs.game_id>=3,'team_id']=3
        self.assertTrue(candidates(games,obs,ctx).empty)

    def test_future_new_player_not_candidate(self):
        games,obs,ctx=fixture()
        extra=obs.iloc[[-1]].copy(); extra['player_id']='future'
        result=candidates(games,pd.concat([obs,extra]),ctx)
        self.assertNotIn('future',result.player_id.tolist())

    def test_same_day_games_share_history_cutoff(self):
        games,obs,ctx=fixture()
        games.loc[games.game_id==5,'kickoff']=games.loc[games.game_id==4,'kickoff'].iloc[0]
        before=candidates(games,obs,ctx)
        obs.loc[obs.game_id==4,['yards','volume']]=[999,90]
        after=candidates(games,obs,ctx)
        pd.testing.assert_frame_equal(before[['game_id',*COLS]],after[['game_id',*COLS]])

    def test_stale_candidate_not_forecast(self):
        games,obs,ctx=fixture()
        games.loc[games.game_id==5,'kickoff']=pd.Timestamp('2025-02-01',tz='UTC')
        result=candidates(games,obs,ctx)
        self.assertNotIn(5,result.game_id.tolist())


if __name__=='__main__': unittest.main()
