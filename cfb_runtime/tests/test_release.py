import unittest
import numpy as np
from cfb.release import probability,selection
from cfb.analyze import validate_request
from cfb.play_labels import reconcile_receivers


class ReleaseTests(unittest.TestCase):
    def test_model_selection_requires_development_gain(self):
        self.assertEqual(selection({'trailing5':10,'volume_efficiency':9.9,'boosted_median':10}),'trailing5')
        self.assertEqual(selection({'trailing5':10,'volume_efficiency':9.5,'boosted_median':9}),'boosted_median')

    def test_probability_monotone_and_push_aware(self):
        residuals=np.arange(-50,50,dtype=float)
        low=probability(50,1,40.5,residuals); high=probability(50,1,60.5,residuals)
        self.assertGreater(low['over'],high['over'])
        self.assertAlmostEqual(sum(low.values()),1)
        self.assertEqual(low['push'],0)
        integer=probability(50,1,50,residuals)
        self.assertGreater(integer['push'],0)
        self.assertAlmostEqual(sum(integer.values()),1)
        with self.assertRaises(ValueError): probability(50,1,float('nan'),residuals)
        with self.assertRaises(ValueError): probability(50,1,50,np.full(100,np.nan))

    def test_input_validates_identity_and_stat(self):
        request={'game_id':123,'player_id':'456','stat':'rush yards','line':49.5,'as_of':'2026-09-30T00:00:00Z'}
        self.assertEqual(validate_request(request)[1],'456')
        for key,value in [('game_id',123.5),('player_id',True),('line',float('nan')),('stat','made up'),('as_of','2026-09-30')]:
            with self.assertRaises(ValueError): validate_request({**request,key:value})

    def test_play_labels_require_complete_matching_totals(self):
        events=[{'playId':'1','athleteId':'a','statType':'Reception','stat':10},
                {'playId':'2','athleteId':'b','statType':'Target','stat':1}]
        passed,zeros,_=reconcile_receivers(events,{'a':(1,10)},1,10)
        self.assertTrue(passed); self.assertEqual(zeros,{'b':1})
        self.assertFalse(reconcile_receivers(events,{'a':(1,10)},2,10)[0])
        self.assertFalse(reconcile_receivers(events,{'a':(1,9)},1,10)[0])
        self.assertFalse(reconcile_receivers(events+events,{'a':(1,10)},1,10)[0])
        self.assertFalse(reconcile_receivers(events*1000,{'a':(1,10)},1,10)[0])


if __name__=='__main__': unittest.main()
