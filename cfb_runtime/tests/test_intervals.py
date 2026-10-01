import unittest
import numpy as np
import pandas as pd
from cfb.intervals import residual_quantile,calibrate,bounds,research_reasons,validate_split,evaluate


class IntervalTests(unittest.TestCase):
    def test_finite_sample_order_statistic(self):
        self.assertEqual(residual_quantile([1,2,3,4],.8),4)
        self.assertTrue(np.isinf(residual_quantile([1,2,3,4],.9)))
        with self.assertRaises(ValueError): residual_quantile([np.nan],.8)

    def test_nesting_and_no_zero_clipping(self):
        cal=calibrate(np.arange(100),np.zeros(100),np.ones(100),'absolute')
        lo80,hi80=bounds(cal,[0],[1],.8)
        lo90,hi90=bounds(cal,[0],[1],.9)
        self.assertLessEqual(lo90[0],lo80[0]); self.assertGreaterEqual(hi90[0],hi80[0])
        self.assertLess(lo90[0],0)

    def test_scale_uses_forecast_volume_not_actual(self):
        cal=calibrate(np.ones(100)*10,np.zeros(100),np.ones(100),'volume_scaled')
        low,high=bounds(cal,[5,5],[1,4],.9)
        self.assertAlmostEqual(high[1]-low[1],2*(high[0]-low[0]))

    def test_test_outcomes_only_change_evaluation(self):
        cal=calibrate(np.arange(100),np.zeros(100),np.ones(100),'absolute')
        low,high=bounds(cal,[0],[1],.9)
        out=pd.DataFrame({'actual_yards':[0],'lower_yards':low,'upper_yards':high,'research_eligible':[True]})
        self.assertEqual(evaluate(out)['observed']['coverage'],1)
        out['actual_yards']=1000
        self.assertEqual(evaluate(out)['observed']['coverage'],0)
        np.testing.assert_array_equal(bounds(cal,[0],[1],.9)[0],low)

    def test_missing_outcomes_are_not_covered_zeros(self):
        out=pd.DataFrame({'actual_yards':[np.nan,0],'lower_yards':[-1,-1],'upper_yards':[1,1],'research_eligible':[True,True]})
        metrics=evaluate(out)
        self.assertEqual(metrics['ungraded'],1)
        self.assertEqual(metrics['observed']['n'],1)
        self.assertEqual(metrics['market_eligible_candidates'],0)

    def test_abstention_and_temporal_split(self):
        cal=calibrate(np.arange(100),np.zeros(100),np.ones(100),'volume_scaled')
        self.assertIn('receiving_zero_label_sensitivity',research_reasons(cal,1,'receiving'))
        self.assertIn('unusually_wide_interval',research_reasons(cal,10,'passing'))
        self.assertIn('volume_outside_calibration_support',research_reasons(cal,10,'passing'))
        calibration=pd.DataFrame({'season':[2024],'kickoff':['2025-01-01T00:00:00Z']})
        test=pd.DataFrame({'season':[2025],'data_cutoff':['2025-09-01T00:00:00Z']})
        validate_split(calibration,test,2024,2025)
        test['data_cutoff']='2024-12-31T00:00:00Z'
        with self.assertRaises(ValueError): validate_split(calibration,test,2024,2025)


if __name__=='__main__': unittest.main()
