import unittest
from decimal import Decimal
from pilot_extract import normalize,normalize_method,validate_candidate
from pilot_runtime import period_from_name


class FinancialValidationTests(unittest.TestCase):
    def test_financial_units_and_signed_profit(self):
        self.assertEqual(normalize('9,053,468','人民币百万元','total_assets'),(Decimal('9053468000000'),'yuan'))
        self.assertEqual(normalize('203.51','亿元','total_profit')[0],Decimal('20351000000'))
        self.assertEqual(normalize('(1,234.56)','万元','total_profit')[0],Decimal('-12345600'))
    def test_percentage_not_fraction_or_capped_at_100(self):
        self.assertEqual(normalize('172.84%','%','provision_coverage_ratio'),(Decimal('172.84'),'percent'))
        with self.assertRaises(ValueError):normalize('12.35','未知','capital_adequacy_ratio')
    def test_bad_numeric_and_unit_rejected(self):
        for value,unit in [('NaN','元'),('Infinity','元'),('1e23','元'),('10','美元')]:
            with self.assertRaises(ValueError):normalize(value,unit,'total_assets')
    def test_half_year_does_not_match_annual(self):
        self.assertEqual(period_from_name('浦发2025年半年度报告.pdf')[:2],('2025-01-01','2025-06-30'))
        self.assertEqual(period_from_name('浦发2025年年度报告.pdf')[:2],('2025-01-01','2025-12-31'))
    def test_controlled_method_preserves_semantics(self):
        self.assertEqual(normalize_method('五级分类口径'),'five_category')
        with self.assertRaises(ValueError):normalize_method('自行猜测的方法')
    def test_wrong_period_and_absent_number_rejected(self):
        c=dict(metric_code='total_profit',page=1,period_start='2025-01-01',period_end='2025-03-31',period_basis='year_to_date',entity_scope='accounting_consolidated',accounting_basis='unknown',currency='CNY',method='not_applicable',raw_value='20,155',raw_unit='百万元')
        doc=dict(period_start='2025-01-01',period_end='2025-03-31');pages=[dict(page=1,text='利润总额 20,155 百万元')]
        self.assertEqual(validate_candidate(c,doc,pages)[0],Decimal('20155000000'))
        for change in [dict(raw_value='20,351'),dict(period_start='2025-03-01'),dict(page=2)]:
            with self.assertRaises(ValueError):validate_candidate(dict(c,**change),doc,pages)


if __name__=='__main__':unittest.main()
