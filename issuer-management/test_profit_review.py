import copy,hashlib,unittest
from production_review_profit_disclosures import require_manifest,overlay


class ProfitReviewGuardTests(unittest.TestCase):
    def setUp(self):
        self.item={'review_wrapper_sha256':'a'*64,'image_sha256':'b'*64,'visual_checked':True,'metric_code':'net_profit',
                   'candidate':{'metric_code':'net_profit','entity_scope':'accounting_consolidated','currency':'CNY','method':'not_applicable','period_basis':'year_to_date','period_start':'2025-01-01','period_end':'2025-09-30'}}
    def test_unreviewed_or_unpinned_sources_cannot_run(self):
        require_manifest({'items':[self.item]},'a'*64)
        for field,value in [('visual_checked',False),('image_sha256',''),('review_wrapper_sha256','c'*64)]:
            item=copy.deepcopy(self.item);item[field]=value
            with self.assertRaises(AssertionError):require_manifest({'items':[item]},'a'*64)
        with self.assertRaises(AssertionError):require_manifest({'items':[]},'a'*64)
    def test_wrong_metric_scope_and_quarter_only_amount_rejected(self):
        for field,value in [('metric_code','total_profit'),('entity_scope','standalone'),('period_start','2025-07-01'),('method','not_specified')]:
            item=copy.deepcopy(self.item);item['candidate'][field]=value
            with self.assertRaises(AssertionError):require_manifest({'items':[item]},'a'*64)

    def test_transcription_preserves_ocr_and_does_not_repair_digits(self):
        text='净利润 39,171 （原图本集团当期列，人民币百万元）'
        item=dict(self.item,sha256='doc',page=1,image_transcript={'page':1,'text':text,'sha256':hashlib.sha256(text.encode()).hexdigest()})
        item['candidate']=dict(item['candidate'],raw_value='39,171',original_label='净利润')
        pages={1:{'text':'净利润 39, 171 35,687','page':1,'ocr_artifact':'x.json'}}
        result=overlay(pages,{'sha256':'doc'},[item])
        self.assertEqual(result[1]['original_extracted_text'],pages[1]['text'])
        self.assertEqual(pages[1]['ocr_artifact'],'x.json')
        self.assertNotIn('ocr_artifact',result[1])
        for bad in ['净利润 39 171','净利润 39,172','净利润 139,171']:
            with self.assertRaises(AssertionError):overlay({1:{'text':bad}}, {'sha256':'doc'},[item])


if __name__=='__main__':unittest.main()
