import unittest
from wnba_events.records import event_record

RAW={'away':'GS','home':'ATL','gameID':'20261021_GS@ATL','homePts':'97','awayPts':'71',
     'gameStatus':'Completed','gameStatusCode':'2','gameTime_epoch':'1792627200.0'}

class EventEvidenceTests(unittest.TestCase):
 def record(self,**overrides):return event_record(RAW|overrides,'getWNBAScoresOnly','20261021','r1')
 def test_verified_provider_fixture(self):
  row=self.record();self.assertTrue(row['is_final']);self.assertEqual(row['event_time'].isoformat(),'2026-10-22T00:00:00+00:00')
 def test_live_suspended_missing_time_and_scores_never_final(self):
  for extra in ({'gameStatus':'Live - In Progress'},{'gameStatus':'Suspended'},{'gameStatusCode':'1'},
                {'gameTime_epoch':None},{'homePts':None},{'awayPts':'97'},{'gameTime_epoch':'nan'}):
   self.assertFalse(self.record(**extra)['is_final'])
  self.assertFalse(event_record(RAW,'getWNBAGamesForDate','20261021','r1')['is_final'])
 def test_identity_conflicts_rejected_and_new_runs_retained(self):
  for extra in ({'home':'NY'},{'gameID':'20261020_GS@ATL'}):
   with self.assertRaises(ValueError):self.record(**extra)
  self.assertNotEqual(self.record()['observation_id'],event_record(RAW,'getWNBAScoresOnly','20261021','r2')['observation_id'])

if __name__=='__main__':unittest.main()
