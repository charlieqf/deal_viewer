import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from preinvestment import KINDS, REQUIRED, confirmation_error, current_state, title_selected
from risk_rules import classify, event_key
from acquisition.risk_batch import batch
from acquisition.preinvestment_sources import parse_list, matches
from acquisition.job_order import fair_order


class PreinvestmentTests(unittest.TestCase):
    def test_actual_joint_conditions(self):
        for kind in KINDS:
            facts=dict(has_event=True,issuer_applicable=True,product_name='测试ABS',**{k:True for k in REQUIRED[kind]})
            self.assertIsNone(confirmation_error(kind,facts))
            self.assertEqual(classify(kind,facts,'测试银行',{}),'pending')
            for field in REQUIRED[kind]:
                self.assertIsNotNone(confirmation_error(kind,dict(facts,**{field:False})))
            self.assertIsNotNone(confirmation_error(kind,dict(facts,issuer_applicable=False)))

    def test_contract_terms_archive_only_with_verified_conclusion(self):
        quote='如发生终止事件，可启动后备贷款服务机构；截至报告日未发生此类事件。'
        facts={'has_event':False,'unresolved':False,'evidence':{'conclusion':{'page':1,'quote':quote}}}
        for kind in KINDS:
            self.assertIsNone(classify(kind,facts,'测试银行',{1:quote}))
            self.assertEqual(classify(kind,facts,'测试银行',{}),'pending')
            self.assertEqual(classify(kind,dict(facts,unresolved=True),'测试银行',{1:quote}),'pending')

    def test_current_state_requires_dated_evidence(self):
        facts={'effectiveness':'resolved','effectiveness_as_of':'2026-11-01'}
        self.assertEqual(current_state(facts,date(2026,11,6))['code'],'unknown')
        facts['evidence']={'review':{'page':1,'quote':'恢复备案'}}
        self.assertEqual(current_state(facts,date(2026,11,6))['code'],'resolved')
        self.assertEqual(current_state(facts,date(2026,10,31))['code'],'unknown')

    def test_official_list_schema_and_identity(self):
        html='''<li><a href="./202611/a.pdf">纪律处分决定书（测试银行股份有限公司）</a><i>2026-11-01</i></li>
        <script>createPageHTML(3,0,"index","html")</script>'''
        items,total=parse_list(html,'https://www.amac.org.cn/zlgl/jlcf/scfjg/')
        self.assertEqual(total,3);self.assertEqual(len(items),1)
        issuer={'name':'测试银行股份有限公司','confirmed_aliases':[]}
        self.assertTrue(matches(items[0]['title'],issuer))
        self.assertFalse(matches('纪律处分（测试银行股份有限公司分行）',issuer))
        with self.assertRaises(ValueError):parse_list('<html>Access denied</html>','https://www.amac.org.cn/')

    def test_round_robin_no_starvation_or_loss(self):
        jobs=[{'id':i,'kind':'rating'} for i in range(100)]+[{'id':100,'kind':'default'},{'id':101,'kind':'abs_suspension'}]
        result=fair_order(jobs)
        self.assertEqual({i['kind'] for i in result[:3]},{'rating','default','abs_suspension'})
        self.assertEqual(sorted(i['id'] for i in result),list(range(102)))

    def test_same_week_rollout_preserves_old_intervals(self):
        with tempfile.TemporaryDirectory() as root:
            inventory=[{'id':1}];day=datetime(2026,11,6,tzinfo=timezone.utc)
            original=batch(root,inventory,day)
            old=dict(original);old['intervals']={k:v for k,v in original['intervals'].items() if k.endswith(':rating')}
            path=Path(root)/'batches'/(old['id']+'.json');path.write_text(json.dumps(old))
            value=batch(root,inventory,day)
            self.assertEqual(value['started'],old['started'])
            self.assertEqual(value['intervals']['1:rating'],old['intervals']['1:rating'])
            self.assertIn('1:abs_suspension',value['intervals'])

    def test_titles_and_cross_category_identity(self):
        self.assertFalse(title_selected('maturity_unpaid','普通付息兑付公告'))
        self.assertTrue(title_selected('maturity_unpaid','法定到期日清偿公告'))
        f=dict(party='银行',product_name='ABS1',event_date='2026-11-01')
        self.assertNotEqual(event_key(1,'default',f,'0'*64),event_key(1,'maturity_unpaid',f,'0'*64))


if __name__=='__main__':unittest.main()
