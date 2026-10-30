import copy
import unittest
from ai_review import assess, numeric_token


class ReviewChecks(unittest.TestCase):
    def setUp(self):
        text = '银行 合并报表 企业会计准则 2025年6月30日 单位百万元 总资产 123.45'
        self.pages = {(1, 2): {'native': text, 'ocr': text}}
        self.stored = dict(MetricCode='total_assets', PeriodEnd='2025-06-30', PeriodBasis='instant',
                           EntityScopeCode='accounting_consolidated', AccountingBasisCode='CAS', CurrencyCode='CNY',
                           UnitCode='yuan', NumericValue='123450000')
        self.item = dict(metric_code='total_assets', status='found', raw_value='123.45', raw_unit='百万元',
                         period_end='2025-06-30', period_basis='instant', entity_scope='accounting_consolidated',
                         accounting_basis='CAS', currency='CNY', evidence={})
        for key, quote in dict(value='总资产 123.45', period='2025年6月30日', unit='单位百万元',
                               scope='合并报表', basis='企业会计准则', issuer='银行').items():
            self.item['evidence'][key] = dict(document_id=1, page=2, quote=quote)

    def test_evidenced_equal_value(self):
        self.assertEqual(assess(self.item, self.stored, self.pages), [])

    def test_wrong_unit_and_period_are_rejected(self):
        item = copy.deepcopy(self.item); item.update(raw_unit='万元', period_end='2024-06-30')
        self.assertIn('value_mismatch', assess(item, self.stored, self.pages))
        self.assertIn('period_end_mismatch', assess(item, self.stored, self.pages))

    def test_wrong_scope_rejected(self):
        self.item['entity_scope'] = 'standalone'
        self.assertIn('entity_scope_mismatch', assess(self.item, self.stored, self.pages))

    def test_ocr_disagreement_blocks_pass(self):
        self.pages[(1, 2)]['ocr'] = '总资产 128.45'
        self.assertIn('ocr_numeric_confirmation_missing', assess(self.item, self.stored, self.pages))

    def test_fabricated_quote_rejected(self):
        self.item['evidence']['value']['quote'] = '总资产为123.45'
        self.assertIn('value_quote_unverifiable', assess(self.item, self.stored, self.pages))

    def test_never_join_separate_numeric_cells(self):
        from decimal import Decimal
        self.assertFalse(numeric_token(Decimal('12345'), '123 45'))
        self.assertFalse(numeric_token(Decimal('23.45'), '123.45'))


if __name__ == '__main__': unittest.main()
