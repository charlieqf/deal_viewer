import unittest
from disclosure_review import disclosed_ratio

class DisclosureTests(unittest.TestCase):
    def setUp(self):
        self.doc={'period_end':'2024-03-31'}
        self.item={'period_end':'2024-03-31','page':2,'metric_code':'provision_coverage_ratio','raw_value':'245.51'}
        self.page={'page':2,'text':'项目 2024年3月31日 2023年12月31日\n不良贷款率(%) 1.07 1.07 持平\n拨备覆盖率(%) 245.51 245.21 提高0.30个百分点'}
    def test_disclosed_value_does_not_claim_group_scope(self):
        c=disclosed_ratio(self.item,self.doc,self.page)
        self.assertEqual(c['entity_scope'],'unknown')
        self.assertEqual(c['raw_value'],'245.51')
    def test_prior_value_or_wrong_period_is_rejected(self):
        for delta in ({'raw_value':'245.21'},{'period_end':'2025-03-31'},{'page':3}):
            with self.assertRaises(ValueError):disclosed_ratio(dict(self.item,**delta),self.doc,self.page)
    def test_other_table_date_cannot_establish_current_column(self):
        page=dict(self.page,text=self.page['text'].replace('2024年3月31日 2023年12月31日','2023年12月31日 2024年3月31日'))
        with self.assertRaises(ValueError):disclosed_ratio(self.item,self.doc,page)
    def test_explicit_scope_is_not_downgraded_to_unknown(self):
        page=dict(self.page,text=self.page['text'].replace('拨备覆盖率','本行拨备覆盖率'))
        with self.assertRaises(ValueError):disclosed_ratio(self.item,self.doc,page)
    def test_segment_table_is_never_issuer_disclosure(self):
        page=dict(self.page,text='县域金融业务主要财务指标\n'+self.page['text'])
        with self.assertRaises(ValueError):disclosed_ratio(self.item,self.doc,page)

if __name__=='__main__':unittest.main()
