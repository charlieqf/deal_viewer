import copy
import unittest
from ai_review_batch import assess,parse,review_batches,review_payload,split_recovery,Runner,db,stamp


class BatchReviewTests(unittest.TestCase):
    def test_gateway_day_limit_is_waited_and_retried_not_a_financial_failure(self):
        import json,tempfile,threading
        from pathlib import Path
        from unittest.mock import Mock,patch
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner.__new__(Runner);runner.root=Path(folder)
            runner.lock=threading.Lock();runner.next_start=0;runner.config={'api_key':'test-only'}
            limited=Mock(status_code=429,headers={'x-request-id':'quota-test'})
            limited.json.return_value={'error':{'retryable':True,'limit_kind':'request_day','retry_after_seconds':3600}}
            success=Mock(status_code=200,headers={'x-request-id':'success-test'})
            success.json.return_value={'choices':[]}
            with patch.object(runner,'wait_for_gateway') as wait,patch('requests.post',side_effect=[limited,success]) as post:
                result=runner.response('test-hash',{})
            self.assertEqual(result['status'],200);self.assertEqual(post.call_count,2);self.assertEqual(wait.call_count,2)
            self.assertGreater(runner.next_start,0)
            state=json.loads((runner.root/'gateway-wait.json').read_text())
            self.assertTrue(state['automatic_resume']);self.assertEqual(state['limit_kind'],'request_day')
            self.assertEqual(len(list((runner.root/'attempts').glob('*.json'))),2)
            # A successful cached response must never be sent again.
            with patch('requests.post') as post:self.assertEqual(runner.response('test-hash',{})['status'],200)
            post.assert_not_called()

    def test_existing_451_calls_do_not_block_next_review(self):
        import json,tempfile,threading
        from pathlib import Path
        from unittest.mock import Mock,patch
        with tempfile.TemporaryDirectory() as folder:
            runner=Runner.__new__(Runner);runner.root=Path(folder)
            runner.lock=threading.Lock();runner.next_start=0;runner.config={'api_key':'test-only'}
            journal=db(runner.root);now=stamp()
            journal.executemany('INSERT INTO calls VALUES(?,?,?,?)',[(str(i),now[:10],'saved',now) for i in range(451)])
            journal.commit();journal.close()
            records=[{'FinancialRecordId':1,'metrics':[{'MetricCode':'total_assets','ValueStatus':'reported'}]}]
            ctx={'records':[{'record_id':1,'requested':['total_assets']}],'pages':[]}
            body={'choices':[{'finish_reason':'stop','message':{'content':json.dumps({'refs':{},'items':[{'record_id':1,'metric_code':'total_assets','evidence':{}}]})}}]}
            response=Mock(status_code=200,headers={'x-request-id':'test-request'});response.json.return_value=body
            with patch('ai_review_batch.context',return_value=ctx),patch('requests.post',return_value=response) as post:
                result=runner.call('goldencode',ctx,records,'initial')
            self.assertIn((1,'total_assets'),result);post.assert_called_once()
            journal=db(runner.root);self.assertEqual(journal.execute('SELECT COUNT(*) FROM calls').fetchone()[0],452);journal.close()

    def test_request_specific_schema_and_split_recovery(self):
        ctx={'records':[{'record_id':1,'requested':['total_assets','net_profit']}]}
        schema=review_payload(ctx)['tools'][0]['function']['parameters']
        self.assertEqual(schema['properties']['items']['minItems'],2)
        self.assertEqual(schema['properties']['items']['maxItems'],2)
        self.assertEqual(schema['properties']['refs']['maxProperties'],12)
        self.assertFalse(schema['additionalProperties'])
        record={'FinancialRecordId':1,'metrics':[{'MetricCode':str(i),'ValueStatus':'reported'} for i in range(5)]}
        halves=split_recovery([record]);self.assertEqual([len(b[0]['metrics']) for b in halves],[3,2])
        self.assertEqual([m['MetricCode'] for b in halves for m in b[0]['metrics']],list(map(str,range(5))))
        self.assertEqual(split_recovery([dict(record,metrics=record['metrics'][:1])]),[])

    def test_duplicate_keys_and_runaway_references_rejected(self):
        import json
        records=[{'FinancialRecordId':1,'metrics':[{'MetricCode':'total_assets','ValueStatus':'reported'}]}]
        item={'record_id':1,'metric_code':'total_assets','evidence':{}}
        def body(raw):return {'choices':[{'finish_reason':'stop','message':{'content':raw}}]}
        duplicate='{"refs":{},"refs":{},"items":'+json.dumps([item])+'}'
        with self.assertRaisesRegex(ValueError,'duplicate_json_key'):parse(body(duplicate),records)
        ref={'document_id':1,'page':1,'quote':'x'*501}
        with self.assertRaisesRegex(ValueError,'quote_limit'):parse(body(json.dumps({'refs':{'r':ref},'items':[item]})),records)
        refs={str(i):{'document_id':1,'page':1,'quote':'x'} for i in range(7)}
        with self.assertRaisesRegex(ValueError,'review_count_limit'):parse(body(json.dumps({'refs':refs,'items':[item]})),records)
        with self.assertRaisesRegex(ValueError,'reason_limit'):parse(body(json.dumps({'refs':{},'items':[dict(item,reason='x'*81)]})),records)

    def test_output_budget_includes_reasoning_and_batches_cover_exactly_once(self):
        records=[{'FinancialRecordId':n,'metrics':[{'MetricCode':str(i),'ValueStatus':'reported'} for i in range(9)]+[{'MetricCode':'absent','ValueStatus':'unresolved'}]} for n in (1,2)]
        batches=review_batches(records)
        self.assertEqual([len(b[0]['metrics']) for b in batches],[8,1,8,1])
        keys=[(b[0]['FinancialRecordId'],m['MetricCode']) for b in batches for m in b[0]['metrics']]
        self.assertEqual(len(keys),len(set(keys)));self.assertEqual(len(keys),18)
        payload=review_payload({})
        self.assertEqual(payload['max_tokens'],64000);self.assertEqual(payload['reasoning_effort'],'low')
        self.assertNotIn('max_completion_tokens',payload)

    def setUp(self):
        text='银行 本行 2025年6月30日 单位% 不良贷款率 1.81'
        self.pages={(1,2):{'native':text,'ocr':''}}
        self.stored=dict(MetricCode='nonperforming_loan_ratio',PeriodEnd='2025-06-30',PeriodBasis='instant',EntityScopeCode='standalone',UnitCode='percent',NumericValue='1.81')
        self.item=dict(metric_code='nonperforming_loan_ratio',status='found',raw_value='1.81',raw_unit='%',period_end='2025-06-30',period_basis='instant',entity_scope='standalone',evidence={})
        for k,q in dict(value='不良贷款率 1.81',period='2025年6月30日',unit='单位%',scope='本行',issuer='银行').items():
            self.item['evidence'][k]=dict(document_id=1,page=2,quote=q)

    def test_native_ratio_no_currency(self):
        self.item['currency']='CNY';self.item['accounting_basis']='IFRS'
        self.assertEqual(assess(self.item,self.stored,self.pages,'bank'),[])

    def test_unknown_scope_default_is_only_standalone_bank(self):
        self.item['entity_scope']='unknown';self.item['evidence'].pop('scope')
        self.assertEqual(assess(self.item,self.stored,self.pages,'bank'),[])
        stored=dict(self.stored,EntityScopeCode='accounting_consolidated')
        self.assertIn('scope_mismatch',assess(self.item,stored,self.pages,'bank'))
        self.assertIn('scope_unknown',assess(self.item,self.stored,self.pages,'amc'))

    def test_wrong_document_rejected(self):
        self.item['evidence']['value']['document_id']=99
        self.assertIn('value_quote_unverifiable',assess(self.item,self.stored,self.pages,'bank'))

    def test_wrong_value_period_unit_rejected(self):
        item=copy.deepcopy(self.item);item.update(raw_value='18.1',period_end='2024-06-30',raw_unit='元')
        errors=assess(item,self.stored,self.pages,'bank')
        for e in ['value_mismatch','period_end_mismatch','unit_kind_mismatch']:self.assertIn(e,errors)

    def test_ocr_only_requires_visual(self):
        self.pages[(1,2)]['ocr']=self.pages[(1,2)]['native'];self.pages[(1,2)]['native']=''
        self.assertIn('visual_check_required',assess(self.item,self.stored,self.pages,'bank'))

    def test_duplicate_or_missing_model_item_rejected(self):
        import json
        records=[{'FinancialRecordId':1,'metrics':[{'MetricCode':'total_assets','ValueStatus':'reported'}]}]
        item={'record_id':1,'metric_code':'total_assets','evidence':{}}
        for items in [[],[item,item]]:
            body={'choices':[{'finish_reason':'stop','message':{'content':json.dumps({'refs':{},'items':items})}}]}
            with self.assertRaises(ValueError):parse(body,records)

    def test_structured_tool_result_and_wrong_tool(self):
        import json
        records=[{'FinancialRecordId':1,'metrics':[{'MetricCode':'total_assets','ValueStatus':'reported'}]}]
        result={'refs':{'r1':{'document_id':10,'page':2,'quote':'资产总计100'}},
                'items':[{'record_id':1,'metric_code':'total_assets','evidence':{'value':'r1'}}]}
        body={'choices':[{'finish_reason':'tool_calls','message':{'tool_calls':[{'function':{
            'name':'submit_financial_review','arguments':json.dumps(result)}}]}}]}
        self.assertEqual(parse(body,records)[(1,'total_assets')]['evidence']['value']['page'],2)
        body['choices'][0]['message']['tool_calls'][0]['function']['name']='change_database'
        with self.assertRaises(ValueError):parse(body,records)


if __name__=='__main__':unittest.main()
