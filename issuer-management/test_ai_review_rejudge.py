import json
import tempfile
import unittest
from pathlib import Path
import ai_review_rejudge as rj
import ai_review_writeback as wb

PAGE='银行 本行按照中国企业会计准则编制财务报告 2025年6月30日 (单位:人民币元) 资产总计 181 净利润 50 利润总额 30'
OBSERVATIONS={'total_assets':11,'net_profit':12,'operating_revenue':13,'total_profit':14}
STORED=dict(ValueStatus='reported',UnitCode='CNY',PeriodEnd='2025-06-30',PeriodBasis='instant',
            EntityScopeCode='standalone',AccountingBasisCode='unknown',CurrencyCode='CNY')


def answer(metric,value,unit_quote='（单位：人民币元）'):
    quote={'total_assets':'资产总计 181','net_profit':'净利润 '+value,'total_profit':'利润总额 '+value}.get(metric,'营业收入 '+value)
    return {'record_id':5,'metric_code':metric,'status':'found','raw_value':value,'raw_unit':'元','period_end':'2025-06-30',
            'period_basis':'instant','entity_scope':'unknown','accounting_basis':'unknown','currency':'CNY',
            'evidence':{k:{'document_id':7,'page':2,'quote':q} for k,q in
                        dict(value=quote,period='2025年6月30日',unit=unit_quote,issuer='银行').items()}}


def source_run(folder,complete=True):
    root=Path(folder)/'full-v9'
    for d in ('results','documents'):(root/d).mkdir(parents=True)
    (root/'verification.json').write_text(json.dumps({'review_complete':complete,'business_writes':False}))
    metrics=[dict(STORED,MetricCode='total_assets',ObservationId=11,NumericValue='181'),
             dict(STORED,MetricCode='net_profit',ObservationId=12,NumericValue='50',PeriodBasis='instant'),
             dict(STORED,MetricCode='operating_revenue',ObservationId=13,NumericValue='9'),
             dict(STORED,MetricCode='total_profit',ObservationId=14,NumericValue='30')]
    record={'FinancialRecordId':5,'IssuerId':3,'LegalName':'银行','InstitutionType':'bank','PeriodEnd':'2025-06-30',
            'metrics':metrics,'evidence_pages':{'7':[2]}}
    (root/'snapshot.json').write_text(json.dumps({'version':'issuer-batch-review-v9','records':[record],'groups':[{'id':'5','record_ids':[5]}]}))
    pages=[{'document_id':7,'page':1,'native':'银行 半年度报告 '+'本行按照中国企业会计准则编制财务报告。'*3,'ocr':''},{'document_id':7,'page':2,'native':PAGE,'ocr':''}]
    (root/'documents/7.json').write_text(json.dumps({'DocumentId':7,'Sha256':'A'*64,'StorageKey':'x.pdf','pages':pages}))
    def finding(metric,status,value,second=None):
        return {'record_id':5,'issuer':'银行','metric':metric,'observation_id':OBSERVATIONS[metric],
                'status':status,'errors':{'goldencode':['unit_quote_unverifiable','basis_mismatch'],'goldencode-d':['basis_mismatch']},
                'findings':{'goldencode':answer(metric,value),'goldencode-d':second or answer(metric,value)}}
    # The second model read the same profit but gave no issuer quote.
    bare=answer('total_profit','30');bare['evidence'].pop('issuer')
    initial=[finding('total_assets','exception','181'),finding('net_profit','exception','60'),finding('operating_revenue','checks_passed','9'),
             finding('total_profit','exception','30',bare)]
    final=[finding('total_assets','exception','181'),finding('net_profit','exception','60'),initial[2],finding('total_profit','exception','30',bare)]
    (root/'results/5.json').write_text(json.dumps({'history':[{'phase':'initial','findings':initial},{'phase':'expanded','findings':final}],'results':final}))
    return root


class RejudgeTests(unittest.TestCase):
    def test_only_former_exceptions_are_rejudged_and_output_feeds_writeback(self):
        with tempfile.TemporaryDirectory() as folder:
            out=Path(folder)/'rejudge'
            root=source_run(folder);report=rj.rejudge([root],out)
            self.assertEqual((report['counts'],report['model_calls']),({'checks_passed':2,'exception':1},0))
            self.assertEqual(report['negative_controls_rejected'],16)
            results={r['metric']:r for r in json.loads((out/'results/full-v9-5.json').read_text(encoding='utf-8'))['results']}
            self.assertEqual(set(results),{'total_assets','net_profit','total_profit'})
            passed=results['total_assets']
            self.assertEqual((passed['status'],passed['judged_on'],passed['domestic_report']),('checks_passed','expanded',True))
            self.assertIn('basis_mismatch',passed['previous_errors']['goldencode'])
            self.assertIn('value_mismatch',results['net_profit']['errors']['goldencode'])
            peer=results['total_profit']
            self.assertEqual((peer['status'],peer['errors']),('checks_passed',{'goldencode':[],'goldencode-d':['issuer_quote_unverifiable']}))
            _,version,items=wb.candidates(out)
            self.assertEqual(version,'issuer-batch-review-v11')
            self.assertEqual([(i['record_id'],i['metric'],i['observation_id']) for i in items],[(5,'total_assets',11),(5,'total_profit',14)])
            with self.assertRaises(ValueError):rj.rejudge([root],out)

    def test_incomplete_source_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ValueError):rj.rejudge([source_run(folder,complete=False)],Path(folder)/'out')


if __name__=='__main__':unittest.main()
