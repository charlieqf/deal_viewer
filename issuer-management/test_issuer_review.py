import unittest
from decimal import Decimal
from issuer_review import checked,validate_eligibility,ReviewError

class ReviewValidationTests(unittest.TestCase):
    def setUp(self):
        self.p=dict(issuer_id='30',expected_record_id='773',document_id='278',page='1',
          request_id='review-test-20261020',period_end='2025-09-30',metric_code='net_profit',
          reviewer='测试核对人',reason='已核对原文当期列',quote='净利润49.88亿元',
          confirmed=True,raw_value='49.88',raw_unit='亿元',scope='unknown',basis='unknown')
    def test_decimal_and_explicit_default(self):
        result=checked(self.p)
        self.assertEqual(result['numeric'],Decimal('4988000000'))
        self.assertEqual(result['scope'],'standalone')
        self.assertTrue(result['scope_defaulted'])
        self.assertEqual(result['period_start'],'2025-01-01')
    def test_bad_values_and_unconfirmed(self):
        for changes in [dict(raw_value='NaN'),dict(raw_value='1e12'),dict(raw_unit='%'),
                        dict(confirmed=False),dict(page=0),dict(scope='regulatory_consolidated'),
                        dict(basis='invented'),dict(period_end='2025-02-30'),dict(reviewer='')]:
            with self.subTest(changes=changes),self.assertRaises(ReviewError):checked(dict(self.p,**changes))
    def test_ratio_bounds_and_regulatory_scope(self):
        p=dict(self.p,metric_code='capital_adequacy_ratio',raw_unit='%',raw_value='13.2',
               scope='regulatory_consolidated',basis='CAS')
        self.assertEqual(checked(p)['basis'],'not_applicable')
        self.assertEqual(checked(p)['period_start'],'2025-09-30')
        with self.assertRaises(ReviewError):checked(dict(p,raw_value='101'))
        with self.assertRaises(ReviewError):checked(dict(p,scope='accounting_consolidated'))
    def test_zero_and_negative_profit(self):
        self.assertEqual(checked(dict(self.p,raw_value='0'))['numeric'],0)
        self.assertEqual(checked(dict(self.p,raw_value='-0.01'))['numeric'],-1000000)
    def test_amc_requires_matching_definition_and_explicit_scope(self):
        issuer=dict(MonitoringStatus='active',InstitutionType='amc')
        p=dict(self.p,scope='accounting_consolidated',definition_confirmed=True)
        validate_eligibility(issuer,checked(p))
        for changes in [dict(definition_confirmed=False),dict(scope='unknown'),
                        dict(metric_code='nonperforming_loan_ratio',raw_value='1',raw_unit='%')]:
            with self.subTest(changes=changes),self.assertRaises(ReviewError):
                validate_eligibility(issuer,checked(dict(p,**changes)))
        with self.assertRaises(ReviewError):
            validate_eligibility(dict(issuer,MonitoringStatus='paused'),checked(p))
        validate_eligibility(dict(issuer,InstitutionType='bank'),checked(self.p))

if __name__=='__main__':unittest.main()
