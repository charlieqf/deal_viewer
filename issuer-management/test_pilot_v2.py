import unittest
from pilot_extract import parse_content
from pilot_v2_context_resolve import reading_order,context_candidates
from pilot_runtime import period_from_name
from pilot_v2_validation import validate,scope_conflict,profit_row_allowed
from unittest.mock import patch


class EvidenceTests(unittest.TestCase):
    def test_profit_attribution_heading_has_its_own_numeric_row(self):
        from pilot_runtime import METRICS
        pages=[self.pages[0],{'page':2,'text':'本集团 本行\n2025年 2024年 2025年 2024年\n净利润 88,013 84,709 78,895 78,030\n净利润归属于：\n母公司股东 86,941 84,156\n少数股东 1,072 553'},self.pages[2]]
        with patch('pilot_extract.METRICS',METRICS+['net_profit','net_profit_attributable_to_parent']):
            validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value='86,941'),self.doc,pages)
            for raw in ('88,013','1,072'):
                with self.assertRaises(ValueError):validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value=raw),self.doc,pages)
    def test_continuing_profit_cannot_cover_discontinued_operations(self):
        text='信達證券構成終止經營。\n持續經營活動稅前利潤╱（虧損） 130.9 (3,722.0)'
        self.assertTrue(scope_conflict('total_profit','accounting_consolidated','130.9',text))
        self.assertFalse(scope_conflict('total_profit','accounting_consolidated','130.9',text.replace('信達證券構成終止經營。','本期无终止经营活动。')))
        self.assertFalse(scope_conflict('total_profit','accounting_consolidated','999.9',text+'\n利润总额 999.9'))
    def test_parent_label_split_inside_profit_word(self):
        from pilot_runtime import METRICS
        pages=[self.pages[0],{'page':2,'text':'本集团 本银行\n2025年 2024年 2025年 2024年\n净利润 9,996,488 8,326,253 9,468,829 7,844,162\n按所有者归属分类\n归属于母公司股东的净\n利润 9,996,488 8,326,253\n归属于母公司股东的综合\n收益总额 10,999,236 9,392,199'},self.pages[2]]
        with patch('pilot_extract.METRICS',METRICS+['net_profit','net_profit_attributable_to_parent']):
            validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value='9,996,488'),self.doc,pages)
            for value in ('10,999,236','9,468,829'):
                with self.assertRaises(ValueError):validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value=value),self.doc,pages)
    def test_separate_attribution_heading_keeps_profit_types_distinct(self):
        from pilot_runtime import METRICS
        for separator in ('：\n ', ': ', '\n '):
            text='本集团 本行\n2025年 2024年 2025年 2024年\n净利润 75,379 76,437 69,000 68,000\n归属于'+separator+'本行股东的净利润 74,743 75,752 68,364 67,315\n少数股东的净利润 636 685 636 685'
            pages=[self.pages[0],{'page':2,'text':text},self.pages[2]]
            with patch('pilot_extract.METRICS',METRICS+['net_profit','net_profit_attributable_to_parent']):
                validate(dict(self.c,metric_code='net_profit',raw_value='75,379'),self.doc,pages)
                validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value='74,743'),self.doc,pages)
                for code,value in [('net_profit','74,743'),('net_profit_attributable_to_parent','75,379'),('net_profit_attributable_to_parent','636'),('net_profit_attributable_to_parent','68,364')]:
                    with self.assertRaises(ValueError):validate(dict(self.c,metric_code=code,raw_value=value),self.doc,pages)
                for replacement in ('本行普通股股东的净利润','本行股东的综合收益','少数股东的净利润'):
                    pages[1]['text']=text.replace('本行股东的净利润',replacement)
                    with self.assertRaises(ValueError):validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value='74,743'),self.doc,pages)
    def test_ordered_capital_sentence_does_not_take_tier_one_value(self):
        from pilot_v2_validation import ordered_capital_value
        text='资本充足率、一级资本充足率、核心一级资本充足率分别为18.40%、13.77%、11.37%。'
        self.assertTrue(ordered_capital_value(text,'18.40'))
        self.assertFalse(ordered_capital_value(text,'13.77'))
        self.assertFalse(ordered_capital_value(text,'11.37'))
        self.assertFalse(ordered_capital_value(text.replace('分别为','分别同比增加'),'18.40'))
    def test_numbered_wrapped_parent_profit_and_bank_header(self):
        from pilot_runtime import METRICS
        pages=[self.pages[0],{'page':2,'text':'本集团 本银行\n2025年 2024年 2025年 2024年\n五、净利润 77,854 77,491 71,611 70,948\n1.归属于母公司股东的\n净利润 77,469 77,205 71,611 70,948'},self.pages[2]]
        with patch('pilot_extract.METRICS',METRICS+['net_profit','net_profit_attributable_to_parent']):
            validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value='77,469'),self.doc,pages)
            for code,value in [('net_profit','77,469'),('net_profit_attributable_to_parent','71,611')]:
                with self.assertRaises(ValueError):validate(dict(self.c,metric_code=code,raw_value=value),self.doc,pages)
            pages[1]['text']=pages[1]['text'].replace('母公司股东','母公司普通股股东')
            with self.assertRaises(ValueError):validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value='77,469'),self.doc,pages)
    def test_quarter_and_ytd_columns_are_not_group_and_bank_columns(self):
        from pilot_runtime import METRICS
        pages=[self.pages[0],{'page':2,'text':'本行及子公司合并数据，以人民币列示。\n2025年7-9月 同比增幅 2025年1-9月 同比增幅\n归属于银行股东的净利润 20,000 3.50 45,000 0.22'},self.pages[2]]
        with patch('pilot_extract.METRICS',METRICS+['net_profit','net_profit_attributable_to_parent']):
            validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value='45,000'),self.doc,pages)
    def test_wrapped_parent_profit_is_not_consolidated_net_profit(self):
        from pilot_runtime import METRICS
        pages=[self.pages[0],{'page':2,'text':'本集团 本行\n2025年 2024年 2025年 2024年\n五、净利润 45,000 42,000 38,000 36,000\n归属于母公司股东的\n净利润 40,000 37,000 35,000 33,000\n少数股东损益 5,000'},self.pages[2]]
        with patch('pilot_extract.METRICS',METRICS+['net_profit','net_profit_attributable_to_parent']):
            validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value='40,000'),self.doc,pages)
            for code,value in [('net_profit','40,000'),('net_profit_attributable_to_parent','35,000')]:
                with self.assertRaises(ValueError):validate(dict(self.c,metric_code=code,raw_value=value),self.doc,pages)
            pages[1]['text']=pages[1]['text'].replace('股东的\n净利润','股东\n的净利润')
            validate(dict(self.c,metric_code='net_profit_attributable_to_parent',raw_value='40,000'),self.doc,pages)
            with self.assertRaises(ValueError):validate(dict(self.c,metric_code='net_profit',raw_value='40,000'),self.doc,pages)
            pages[1]['text']=pages[1]['text'].replace('母公司股东','母公司普通股股东')
            for code in ['net_profit','net_profit_attributable_to_parent']:
                with self.assertRaises(ValueError):validate(dict(self.c,metric_code=code,raw_value='40,000'),self.doc,pages)
    def test_revenue_section_subtotal_uses_printed_total_not_component(self):
        pages=[self.pages[0],{'page':2,'text':'本集团 本行\n2025年 2024年 2025年 2024年\n一、营业收入\n利息净收入 40,000 39,000 38,000 37,000\n手续费收入 13,374 11,000 10,000 9,000\n小计 53,374 50,000 48,000 46,000\n二、营业支出\n小计 30,000 28,000 26,000 25,000'},self.pages[2]]
        validate(dict(self.c,metric_code='operating_revenue'),self.doc,pages)
        for value in ('40,000','48,000','30,000'):
            with self.assertRaises(ValueError):validate(dict(self.c,metric_code='operating_revenue',raw_value=value),self.doc,pages)
    def test_profit_excludes_common_share_and_partial_operations(self):
        for label in ('归属于母公司普通股股东的净利润','持续经营净利润','终止经营净利润','扣除非经常性损益后净利润','少数股东净利润'):
            self.assertFalse(profit_row_allowed('net_profit',label))
            self.assertFalse(profit_row_allowed('net_profit_attributable_to_parent',label))
        self.assertTrue(profit_row_allowed('net_profit_attributable_to_parent','归属于银行股东的净利润'))
    def test_ifrs_full_profit_and_attribution_exclude_partial_rows(self):
        from pilot_runtime import METRICS
        pages=[self.pages[0],{'page':2,'text':'持續經營活動本期間╱年度利潤 (349.5)\n終止經營活動本期間╱年度稅後利潤 1,097.3\n本期間╱年度利潤 747.8\n利潤歸於：\n－本公司股東 784.4\n－非控制性權益 (36.6)'},self.pages[2]]
        with patch('pilot_extract.METRICS', METRICS+['net_profit','net_profit_attributable_to_parent']):
            for code,value in [('net_profit','747.8'),('net_profit_attributable_to_parent','784.4')]:
                validate(dict(self.c,metric_code=code,raw_value=value),self.doc,pages)
            for code,value in [('net_profit','784.4'),('net_profit','1,097.3'),('net_profit','(349.5)'),('net_profit_attributable_to_parent','747.8'),('net_profit_attributable_to_parent','(36.6)')]:
                with self.assertRaises(ValueError):validate(dict(self.c,metric_code=code,raw_value=value),self.doc,pages)
    def test_profit_types_and_parent_bank_are_not_interchangeable(self):
        from pilot_runtime import METRICS
        pages=[self.pages[0],{'page':2,'text':'本集团 本行\n2025年 2024年 2025年 2024年\n利润总额 60,000 50,000 49,000 40,000\n净利润 45,000 42,000 38,000 36,000\n归属于母公司股东的净利润 40,000 37,000 35,000 33,000\n少数股东损益 5,000 5,000 3,000 3,000'},self.pages[2]]
        with patch('pilot_extract.METRICS', METRICS+['net_profit','net_profit_attributable_to_parent']):
            for code,value in [('net_profit','45,000'),('net_profit_attributable_to_parent','40,000')]:
                validate(dict(self.c,metric_code=code,raw_value=value),self.doc,pages)
            for code,value in [('net_profit','40,000'),('net_profit','38,000'),('net_profit','5,000'),('net_profit_attributable_to_parent','45,000'),('net_profit_attributable_to_parent','35,000')]:
                with self.assertRaises(ValueError):validate(dict(self.c,metric_code=code,raw_value=value),self.doc,pages)
            split=[pages[0],dict(page=2,text='净利润\n归属于母公司股东的净利润 40,000'),pages[2]]
            with self.assertRaises(ValueError):validate(dict(self.c,metric_code='net_profit',raw_value='40,000'),self.doc,split)
    def test_malformed_context_candidate_does_not_drop_valid_sibling(self):
        model=dict(issuer_name='测试银行',period_start=self.doc['period_start'],period_end=self.doc['period_end'],candidates=[dict(period_end='2025-12-31'),None,self.c])
        self.assertEqual(context_candidates(model,self.doc,'测试银行'),[(3,self.c)])
        self.assertEqual(context_candidates(dict(model,issuer_name='其他银行'),self.doc,'测试银行'),[])
        self.assertEqual(context_candidates(dict(model,period_end='2024-12-31'),self.doc,'测试银行'),[])
    def test_company_table_intro_overrides_unrelated_group_strategy(self):
        text='結合集團整體發展戰略。下表載列於所示日期本公司資本充足率、資本淨額及風險加權資產情況。於2026年6月30日（人民幣百萬元）核心一級資本充足率9.04 一級資本充足率13.45 資本充足率13.45'
        self.assertTrue(scope_conflict('capital_adequacy_ratio','regulatory_consolidated','13.45',text))
        self.assertFalse(scope_conflict('capital_adequacy_ratio','standalone','13.45',text))
    def test_unrelated_group_prose_cannot_override_company_ratio(self):
        text='本集團認真貫徹落實監管。本公司的資本充足率分別為15.69%及16.52%。'
        self.assertTrue(scope_conflict('capital_adequacy_ratio','regulatory_consolidated','16.52',text))
        self.assertFalse(scope_conflict('capital_adequacy_ratio','standalone','16.52',text))
        self.assertFalse(scope_conflict('capital_adequacy_ratio','regulatory_consolidated','16.52','本公司开展审计。本集团资本充足率为16.52%。'))
    def test_fenced_json_recovers_without_another_model_call(self):
        self.assertEqual(parse_content('```json\n{"ok":true}\n```'),{'ok':True})
        with self.assertRaises(ValueError):parse_content('not JSON')
    def test_slanted_scan_row_keeps_label_and_four_columns(self):
        blocks=[{'text':'22,842','box':[797,1437,876,1464]}, {'text':'21,914','box':[978,1434,1058,1461]}, {'text':'20,525','box':[1157,1432,1236,1460]}, {'text':'20,149','box':[1335,1429,1418,1457]}, {'text':'四、利润总额','box':[194,1441,329,1472]}]
        self.assertEqual(reading_order(blocks),'四、利润总额 22,842 21,914 20,525 20,149')
    def setUp(self):
        self.doc={'period_start':'2025-01-01','period_end':'2025-12-31'}
        self.c=dict(metric_code='total_profit',raw_value='53,374',raw_unit='人民币百万元',
                    period_start='2025-01-01',period_end='2025-12-31',period_basis='year_to_date',
                    entity_scope='accounting_consolidated',accounting_basis='CAS',currency='CNY',method='not_specified',
                    page=2,unit_page=1,scope_page=1,basis_page=3,basis_quote='本财务报表按照企业会计准则编制')
        self.pages=[{'page':1,'text':'合并利润表 人民币百万元 2025年度'},
                    {'page':2,'text':'利润总额 53,374 50,000\n净利润 45,000 42,000'},
                    {'page':3,'text':'本财务报表按照企业会计准则编制，真实完整。'}]
    def test_citic_audited_filename(self):
        self.assertEqual(period_from_name('中信银行股份有限公司2024年经审计的财务报告.pdf'),('2024-01-01','2024-12-31','audit_report'))
        with self.assertRaises(ValueError):period_from_name('银行2025年公告.pdf')
    def test_full_inventory_period_variants(self):
        for filename in ('东方2025年二季度财务报表.pdf','信达2025年中期报告.pdf','广发2025年上半年信息披露报告.pdf'):
            self.assertEqual(period_from_name(filename)[:2],('2025-01-01','2025-06-30'))
        for filename in ('民生2025年经审计财务报表及审计报告.pdf','华夏2025年经审计财务报告.pdf'):
            self.assertEqual(period_from_name(filename),('2025-01-01','2025-12-31','audit_report'))
    def test_context_keeps_actual_unit_and_basis_pages(self):
        self.assertEqual(validate(self.c,self.doc,self.pages)[3],{1,2,3})
    def test_traditional_evidence_preserves_literal_basis_quote(self):
        pages=[{'page':1,'text':'合併利潤表 人民幣百萬元 2025年度'},{'page':2,'text':'利潤總額 53,374 50,000'},{'page':3,'text':'本財務報表按照國際財務報告準則編製'}]
        c=dict(self.c,raw_unit='人民幣百萬元',accounting_basis='IFRS',basis_quote='本財務報表按照國際財務報告準則編製')
        self.assertEqual(str(validate(c,self.doc,pages)[0]),'53374000000')
        with self.assertRaises(ValueError):validate(dict(c,basis_quote='本财务报表按照国际财务报告准则编制'),self.doc,pages)
    def test_unsubstantiated_basis_is_not_accepted(self):
        for delta in [{'basis_page':9},{'basis_quote':'按照国际财务报告准则编制'},{'unit_page':3},{'scope_page':3}]:
            with self.assertRaises(ValueError):validate(dict(self.c,**delta),self.doc,self.pages)
    def test_separators_are_not_repaired(self):
        pages=[dict(p,text=p['text'].replace('53,374','53.374')) for p in self.pages]
        with self.assertRaises(ValueError):validate(self.c,self.doc,pages)
    def test_number_elsewhere_on_page_is_rejected(self):
        pages=[self.pages[0],{'page':2,'text':'利润总额 42,000\n一\n二\n三\n四\n净利润 53,374'},self.pages[2]]
        with self.assertRaises(ValueError):validate(self.c,self.doc,pages)
    def test_label_split_across_native_lines(self):
        pages=[self.pages[0],{'page':2,'text':'利润总\n额 53,374 50,000\n净利润 45,000'},self.pages[2]]
        validate(self.c,self.doc,pages)
    def test_adjacent_net_profit_is_not_total_profit(self):
        pages=[self.pages[0],{'page':2,'text':'利润总额 42,000\n净利润 53,374'},self.pages[2]]
        with self.assertRaises(ValueError):validate(self.c,self.doc,pages)
    def test_group_and_bank_column_swap(self):
        pages=[self.pages[0],{'page':2,'text':'本集团 本行\n2025年 2024年 2025年 2024年\n利润总额 53,374 50,000 48,000 46,000'},self.pages[2]]
        validate(self.c,self.doc,pages)
        with self.assertRaises(ValueError):validate(dict(self.c,raw_value='48,000'),self.doc,pages)
        pages[1]['text']=pages[1]['text'].replace('本集团 本行','合并 银行')
        validate(self.c,self.doc,pages)
        with self.assertRaises(ValueError):validate(dict(self.c,raw_value='48,000'),self.doc,pages)
    def test_accounting_and_regulatory_scope_are_separate(self):
        with self.assertRaises(ValueError):validate(dict(self.c,entity_scope='regulatory_consolidated'),self.doc,self.pages)
    def test_regulatory_consolidated_table_header(self):
        c=dict(self.c,metric_code='capital_adequacy_ratio',raw_value='13.71',raw_unit='%',period_start='2025-12-31',period_basis='instant',entity_scope='regulatory_consolidated',accounting_basis='not_applicable',currency='not_applicable')
        pages=[{'page':1,'text':'2025年12月31日 并表 非并表 单位：%'},{'page':2,'text':'资本充足率 13.71 13.40'}]
        validate(c,self.doc,pages)
        pages[0]['text']='2025年12月31日 非并表 单位：%'
        with self.assertRaises(ValueError):validate(c,self.doc,pages)


if __name__=='__main__':unittest.main()
