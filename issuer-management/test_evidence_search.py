import unittest
from evidence_search import choose_report,classify_gap,context_pages,metric_hits
from pilot_v2_prepare import choose_pages


class RetrievalRegressionTests(unittest.TestCase):
    def setUp(self):
        self.reports=[dict(sha256='a',document_id=1,document_type='annual_report'),dict(sha256='b',document_id=2,document_type='audit_report')]
        self.pages={'a':{1:dict(text='本集团资产总计人民币100亿元；这是年度摘要。',method='native_text')},
                    'b':{1:dict(text='资本充足率 17.23%',method='paddleocr')}}

    def test_other_same_period_pdf_is_searched(self):
        doc,hits=choose_report(self.reports,self.pages,'capital_adequacy_ratio')
        self.assertEqual(doc['document_id'],2)
        self.assertEqual(hits,[(0,1)])  # Low-score scan must not be silently dropped.

    def test_missing_label_is_material_gap_not_non_disclosure(self):
        gap=classify_gap('provision_coverage_ratio',self.reports,self.pages,[])
        self.assertEqual(gap['code'],'material_label_not_found')
        self.assertEqual(len(gap['searched_documents']),2)
        self.assertIn('不代表未披露',gap['message'])

    def test_scope_rejection_is_not_material_gap(self):
        gap=classify_gap('capital_adequacy_ratio',self.reports,self.pages,[dict(metric='capital_adequacy_ratio',reason='Scope not supported by cited page')])
        self.assertEqual(gap['code'],'scope_pending')

    def test_numeric_rejection_does_not_disappear(self):
        gap=classify_gap('capital_adequacy_ratio',self.reports,self.pages,[dict(metric='capital_adequacy_ratio',reason='Ambiguous or malformed thousands separators')])
        self.assertEqual(gap['code'],'numeric_evidence_pending')

    def test_glossary_retrieved_without_interpreting_ambiguous_scope(self):
        text='释义 本行/本集团 指 中国农业银行股份有限公司，或中国农业银行股份有限公司及其子公司。'
        self.assertEqual(context_pages({6:dict(text=text)}),[6])

    def test_combined_company_statement_survives_page_budget(self):
        pages={n:dict(text='这是报告中的普通背景文字，没有需要检索的财务项目。',method='native_text') for n in range(1,101)}
        pages[90]['text']='合并及公司资产负债表 人民币百万元 资产总计 123,456 110,000。'
        pages[6]['text']='释义 本行/本集团 指 中国农业银行股份有限公司，或中国农业银行股份有限公司及其子公司。'
        chosen,_,_=choose_pages(dict(page_count=100),pages)
        self.assertIn(90,[p['page'] for p in chosen])
        self.assertIn(6,[p['page'] for p in chosen])
        self.assertLessEqual(len(chosen),48)


if __name__=='__main__':unittest.main()
