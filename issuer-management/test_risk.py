import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from risk_rules import *
from acquisition.state import Store
from acquisition.worker import collect
from acquisition.risk_batch import batch


def penalty(amount=None, **extra):
    return dict(party='测试银行',party_type='legal_entity',decision_number='罚字1号',
                amounts_complete=True,amounts_reliable=True,fine_total=amount,unit='元',**extra)


class RiskRulesTests(unittest.TestCase):
    def test_amount_components_and_total_not_doubled(self):
        parts=[dict(item_id=str(i),decision_number='罚字1号',party='测试银行',party_type='legal_entity',kind='fine',amount=v,unit='万元') for i,v in enumerate(('6000','5000'))]
        facts=penalty(amounts=parts+parts)
        self.assertEqual(penalty_total(facts,'测试银行'),(Decimal('110000000'),'fine'))
        facts['fine_total']='80000000'
        self.assertEqual(classify('penalty',facts,'测试银行',{}),'below_threshold')
        parts[0]['party_type']='person';facts['fine_total']=None
        self.assertEqual(penalty_total(facts,'测试银行')[0],Decimal('50000000'))

    def test_thresholds_and_confiscation(self):
        for facts in [penalty('80000000',fine_confiscation_total='110000000'),penalty('100000000'),penalty(fine_confiscation_total='90000000')]:
            self.assertEqual(classify('penalty',facts,'测试银行',{}),'below_threshold')
        self.assertEqual(classify('penalty',penalty(fine_confiscation_total='110000000'),'测试银行',{}),'pending')
        self.assertEqual(classify('penalty',penalty(),'测试银行',{}),'pending')

    def test_default_agent_and_own_obligation(self):
        pages={1:'甲公司未能兑付债券。测试银行仅作为主承销商代发公告。'}
        facts=dict(has_default=True,unresolved=False,own_obligation_abnormal=False,debtor='甲公司',issuer_role='lead_underwriter',
                   evidence={'debtor':{'page':1,'quote':'甲公司未能兑付债券'},'issuer_role':{'page':1,'quote':'测试银行仅作为主承销商代发公告'}})
        self.assertEqual(classify('default',facts,'测试银行',pages),'other_debtor')
        self.assertEqual(classify('default',dict(facts,own_obligation_abnormal=True),'测试银行',pages),'pending')
        self.assertEqual(classify('default',facts,'测试银行',{}),'pending')
        facts=dict(has_default=False,own_obligation_abnormal=False,unresolved=False,evidence={'conclusion':{'page':1,'quote':'已按期兑付，无违约事实'}})
        self.assertIsNone(classify('default',facts,'测试银行',{1:'已按期兑付，无违约事实'}))
        self.assertEqual(classify('default',facts,'测试银行',{}),'pending')

    def test_normalization_and_manual_override(self):
        inventory=[dict(id=1,name='大众汽车金融（中国）有限公司')]
        products=[dict(ProductId=1,ProductSubject=' 大众汽车金融(中国)有限公司 ')]
        self.assertEqual(match_products(products,inventory)[0]['IssuerId'],1)
        manual=dict(ProductId=1,IssuerId=2,MappingSource='manual')
        self.assertEqual(match_products(products,inventory,[manual]),[manual])

    def test_window_stale_and_dates(self):
        self.assertEqual(window_start(date(2024,2,29)),date(2022,2,28))
        now=datetime(2026,11,2,tzinfo=timezone.utc)
        self.assertTrue(source_status(None,{'CheckedAt':now-timedelta(days=15)},now)['red'])
        self.assertFalse(source_status(None,{'CheckedAt':now-timedelta(days=14)},now)['red'])
        self.assertTrue(source_status(None,None,now)['red'])

    def test_title_rejection_does_not_download(self):
        class Adapter:
            def search(self,q):yield {'total':1,'pageItems':[{'id':1,'title':'普通付息兑付公告','paths':['/a.pdf']}]}
            def download(self,a):raise AssertionError('must not download')
        with tempfile.TemporaryDirectory() as root:
            store=Store(root)
            result=collect(store,Adapter(),[dict(id=1,name='测试银行',monitoring_status='active')],kinds=('default',))
            self.assertEqual(result['downloaded'],0)
            self.assertEqual(store.db.execute('SELECT COUNT(*) FROM documents').fetchone()[0],0)
            self.assertGreater(store.db.execute("SELECT COUNT(*) FROM events WHERE event='title_filtered'").fetchone()[0],0)
            store.db.close()
        self.assertTrue(default_title_selected('进展公告','债券未能按期偿付.pdf'))

    def test_two_weeks_retry_and_month_store_untouched(self):
        class Adapter:
            calls=0
            def search(self,q):self.calls+=1;yield {'total':0,'pageItems':[]}
        with tempfile.TemporaryDirectory() as root:
            monthly=Store(Path(root)/'monthly');monthly.start()
            weekly=Store(Path(root)/'risk');inv=[dict(id=1,name='测试银行',monitoring_status='active')];adapter=Adapter()
            first='2026-11-03T00:00:00+00:00';second='2026-11-10T00:00:00+00:00'
            with patch('acquisition.state.now',return_value=first),patch('acquisition.worker.now',return_value=first):
                collect(weekly,adapter,inv,kinds=('rating',),resume=True,boundary=first)
            count=adapter.calls
            with patch('acquisition.state.now',return_value=second),patch('acquisition.worker.now',return_value=second):
                collect(weekly,adapter,inv,kinds=('rating',),resume=True,boundary=second)
                self.assertEqual(adapter.calls,2*count)
                collect(weekly,adapter,inv,kinds=('rating',),resume=True,boundary=second)
                self.assertEqual(adapter.calls,2*count)
            self.assertEqual(monthly.db.execute('SELECT status FROM runs').fetchone()[0],'running')
            weekly.db.close();monthly.db.close()

    def test_batch_freezes_dates_and_next_week_starts(self):
        with tempfile.TemporaryDirectory() as root:
            inv=[{'id':1}];t=datetime(2026,11,3,4,tzinfo=timezone(timedelta(hours=8)))
            one=batch(root,inv,t);retry=batch(root,inv,t+timedelta(days=2));two=batch(root,inv,t+timedelta(days=7))
            self.assertEqual(one,retry);self.assertNotEqual(one['id'],two['id'])
            self.assertEqual(one['intervals']['1:default']['since'],'2024-11-03')

    def test_pbc_structured_rows_separate_person_and_date(self):
        from acquisition.risk_sources import penalty_from_row
        issuer={'name':'测试银行股份有限公司'}
        cells=['1',issuer['name'],'银罚决字〔2026〕68号','违规','罚款106万元','中国人民银行','2026年6月3日','三年','']
        facts=penalty_from_row(cells,issuer)
        self.assertEqual(facts['event_date'],'2026-06-03')
        self.assertEqual(classify('penalty',facts,issuer['name'],{1:'\n'.join(cells)}),'below_threshold')
        cells[1]='程某（'+issuer['name']+'零售平台部）'
        facts=penalty_from_row(cells,issuer)
        self.assertEqual(facts['party_type'],'person')
        self.assertEqual(classify('penalty',facts,issuer['name'],{}),'pending')

    def test_invalid_boolean_cannot_auto_classify(self):
        facts=penalty('80000000');facts['amounts_reliable']='false'
        self.assertEqual(classify('penalty',facts,'测试银行',{}),'pending')

    def test_source_success_requires_all_planned_queries(self):
        from acquisition.risk_import import grouped_checks,planned_queries
        with tempfile.TemporaryDirectory() as root:
            store=Store(root);issuer=dict(id=1,name='测试银行',confirmed_aliases=[])
            queries=sorted(planned_queries(issuer,'rating'))
            for query in queries:
                data=dict(issuer_id=1,kind='rating',query=query,boundary='2026-11-01',checked_at='2026-11-02T00:00:00+00:00',since='2024-11-02',until='2026-11-02',status='failed' if query==queries[0] else 'complete')
                store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',('now','risk_check',json.dumps(data)))
            store.db.commit()
            self.assertEqual(grouped_checks(store,[issuer])[0][1]['status'],'failed')
            data['query']=queries[0];data['status']='complete'
            store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',('now','risk_check',json.dumps(data)));store.db.commit()
            self.assertEqual(grouped_checks(store,[issuer])[0][1]['status'],'success');store.db.close()

if __name__=='__main__':unittest.main()
