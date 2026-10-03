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

    def test_changing_team_still_resets_history(self):
        # A transfer means a new role and system: that history must not carry.
        games,obs,ctx=fixture()
        games.loc[games.game_id>=3,'home_id']=3
        obs.loc[obs.game_id>=3,'team_id']=3
        self.assertTrue(candidates(games,obs,ctx).empty)

    def test_history_carries_across_seasons_on_the_same_team(self):
        # A returning starter is eligible in week one; season-keyed history
        # blacked out the opening weeks of every season.
        games,obs,ctx=fixture()
        games.loc[games.game_id>=3,'season']=2025
        games.loc[games.game_id>=3,'kickoff']=games.loc[games.game_id>=3,'kickoff']+pd.Timedelta(days=330)
        result=candidates(games,obs,ctx)
        self.assertEqual(result.game_id.tolist(),[3,4,5])
        first=result.iloc[0]
        self.assertEqual(first.same_season_history,0.0)
        self.assertEqual(first.in_season_games,0.0)
        self.assertEqual(result.iloc[-1].same_season_history,1.0)

    def test_offseason_gap_limit_still_excludes_a_vanished_player(self):
        # Last seen two seasons ago and never since: carryover must not revive them.
        games,obs,ctx=fixture()
        games.loc[games.game_id>=3,'season']=2026
        games.loc[games.game_id>=3,'kickoff']=games.loc[games.game_id>=3,'kickoff']+pd.Timedelta(days=800)
        self.assertTrue(candidates(games,obs[obs.game_id<3],ctx).empty)

    def test_returning_after_one_offseason_is_still_eligible(self):
        games,obs,ctx=fixture()
        games.loc[games.game_id>=3,'season']=2025
        games.loc[games.game_id>=3,'kickoff']=games.loc[games.game_id>=3,'kickoff']+pd.Timedelta(days=330)
        self.assertEqual(candidates(games,obs[obs.game_id<3],ctx).game_id.tolist(),[3,4,5])

    def test_transfer_does_not_duplicate_a_candidate(self):
        # Cross-season history must not leave a transferred player enumerated for
        # both teams, which produced duplicate primary keys against real data.
        games=pd.DataFrame([dict(game_id=i,season=2024 if i<4 else 2025,
                                 kickoff=pd.Timestamp('2024-09-01',tz='UTC')+pd.Timedelta(days=120*i),
                                 home_id=1,away_id=2) for i in range(8)])
        obs=pd.DataFrame([dict(game_id=i,player_id='p1',team_id=1 if i<4 else 2,player_name='Mover',
                               category='rushing',volume=10,yards=50) for i in range(8)])
        ctx=pd.DataFrame([dict(game_id=i,margin_rating=3,total_rating=50,home_pass_allowance=0,
                               away_pass_allowance=0,home_rush_allowance=0,away_rush_allowance=0) for i in range(8)])
        result=candidates(games,obs,ctx)
        keys=result[['game_id','player_id','category']]
        self.assertEqual(len(keys),len(keys.drop_duplicates()))
        after=result[result.game_id>=7]
        self.assertTrue((after.team_id==2).all())

    def test_participation_label_marks_absent_outcomes(self):
        games,obs,ctx=fixture()
        result=candidates(games,obs[obs.game_id!=5],ctx)
        self.assertEqual(result.loc[result.game_id==5,'participated'].iloc[0],0.0)
        self.assertEqual(result.loc[result.game_id==4,'participated'].iloc[0],1.0)

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
