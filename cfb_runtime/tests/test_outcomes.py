import unittest
from cfb.outcomes import positive_activity,reconcile,classify,recommendation_gate


class OutcomeTests(unittest.TestCase):
    def test_absence_is_not_zero_or_dnp(self):
        self.assertEqual(classify(None,None,False,True),('unresolved_no_positive_evidence',None,None))
        self.assertEqual(classify(None,None,True,False),('unresolved_with_activity',None,None))

    def test_inference_cannot_become_verified(self):
        self.assertEqual(classify(None,None,True,True),('inferred_zero_sensitivity_only',None,0))
        self.assertEqual(classify(0,2,True,True),('explicit_zero_yards',0,None))
        self.assertEqual(classify(-5,2,True,True),('observed_yards',-5,None))

    def test_nonzero_count_required_for_activity(self):
        self.assertFalse(positive_activity('defensive','TOT','0'))
        self.assertTrue(positive_activity('passing','C/ATT','0/2'))
        self.assertTrue(positive_activity('defensive','SACKS','0.5'))
        self.assertFalse(positive_activity('passing','QBR','99'))
        self.assertFalse(positive_activity('receiving','REC','--'))

    def test_reconciliation_requires_both_volume_and_yards(self):
        team={'pass_attempts':30,'pass_yards':250,'rush_attempts':40,'rush_yards':150,
              'payload':{'stats':[{'category':'completionAttempts','stat':'20-30'}]}}
        self.assertTrue(reconcile(team,'receiving',(20,250)))
        self.assertFalse(reconcile(team,'receiving',(20,240)))
        self.assertFalse(reconcile(team,'passing',(29,250)))
        self.assertFalse(reconcile(None,'rushing',(40,150)))

    def test_postgame_evidence_cannot_open_recommendation_gate(self):
        inputs=dict(availability='confirmed',availability_observed_at='2025-09-02T00:00:00Z',cutoff='2025-09-01T00:00:00Z',
                    identity_verified=True,distribution_validated=True,grading_rules_verified=True)
        self.assertFalse(recommendation_gate(**inputs)['allowed'])
        inputs['availability_observed_at']='not-a-time'
        self.assertFalse(recommendation_gate(**inputs)['allowed'])
        inputs['availability_observed_at']='2025-08-31T23:00:00Z'
        inputs['distribution_validated']=False
        self.assertFalse(recommendation_gate(**inputs)['allowed'])


if __name__=='__main__': unittest.main()
