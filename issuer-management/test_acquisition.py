import tempfile
import unittest
from pathlib import Path

from acquisition.chinamoney import allowed_url, attachments, parse_result, check_download_status, DownloadHTTPError, older_page
from acquisition.rating import validate_event, chinese_dates, parse_response
from acquisition.state import Store
from acquisition.worker import collect


class AcquisitionTests(unittest.TestCase):
    def test_private_proxy_config_and_auth_encoding(self):
        import json
        from unittest.mock import patch
        from acquisition.network import proxy_config,requests_proxies
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'proxy.json'
            with patch.dict('os.environ',{'ISSUER_ACQUISITION_PROXY_FILE':str(path)}):
                path.write_text(json.dumps({'enabled':False}))
                self.assertIsNone(proxy_config())
                path.write_text(json.dumps({'enabled':True,'server':'http://proxy.example:8080','username':'u@x','password':'p:x'}))
                self.assertEqual(requests_proxies(proxy_config())['https'],'http://u%40x:p%3Ax@proxy.example:8080')
                path.write_text(json.dumps({'enabled':True,'server':'http://user:password@proxy.example:8080'}))
                with self.assertRaises(ValueError):proxy_config()

    def test_repeated_access_denial_ends_batch_with_checkpoint(self):
        class Adapter:
            count=0
            def search(self,query):
                yield {'total':10,'pageItems':[{'id':i,'title':'example','paths':['/'+str(i)+'.pdf']} for i in range(10)]}
            def download(self,item):
                self.count+=1
                raise DownloadHTTPError(403)
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(tmp);adapter=Adapter()
            result=collect(store,adapter,[{'id':1,'name':'A','monitoring_status':'active'}],kinds=('rating',))
            self.assertTrue(result['access_blocked'])
            self.assertEqual(adapter.count,5)
            self.assertEqual(store.db.execute('SELECT status FROM runs').fetchone()[0],'partial')
            self.assertEqual(store.db.execute('SELECT error FROM searches').fetchone()[0],'source_access_blocked')
            store.db.close()

    def test_public_download_preserves_attachment_priority(self):
        from acquisition.chinamoney import download_api_url
        self.assertTrue(download_api_url({'source_id':'3160198','priority':2}).endswith('contentId=3160198&priority=2'))
        with self.assertRaises(ValueError):download_api_url({'source_id':'1&other=2'})
        with self.assertRaises(ValueError):download_api_url({'source_id':'1','priority':-1})

    def test_public_download_validates_body_and_size(self):
        from unittest.mock import MagicMock,patch
        from acquisition.chinamoney import download_public_pdf
        response=MagicMock();response.__enter__.return_value=response
        response.status_code=200;response.headers={}
        session=MagicMock();session.__enter__.return_value=session;session.get.return_value=response
        with patch('requests.Session',return_value=session):
            response.iter_content.return_value=[b'%PDF-',b'test']
            self.assertEqual(download_public_pdf({'source_id':'1'},20),b'%PDF-test')
            self.assertFalse(session.trust_env)
            response.iter_content.return_value=[b'<html>error</html>']
            with self.assertRaisesRegex(ValueError,'download_not_pdf'):download_public_pdf({'source_id':'1'},100)
            response.iter_content.return_value=[b'%PDF-',b'0123456789']
            with self.assertRaisesRegex(ValueError,'pdf_too_large'):download_public_pdf({'source_id':'1'},10)
            response.status_code=403
            with self.assertRaises(DownloadHTTPError):download_public_pdf({'source_id':'1'},100)

    def test_date_boundary_requires_sorted_known_dates(self):
        self.assertTrue(older_page([{'releaseDate':1703980800000},{'releaseDate':1672502400000}],'2024-01-01'))
        self.assertFalse(older_page([{'releaseDate':1672502400000},{'releaseDate':1703980800000}],'2024-01-01'))
        self.assertFalse(older_page([{'releaseDate':None}],'2024-01-01'))
        self.assertFalse(older_page([{'releaseDate':1704067200000}],'2024-01-01'))
    def test_blocked_download_is_not_reported_as_timeout(self):
        with self.assertRaises(DownloadHTTPError) as error:check_download_status(403)
        self.assertEqual(str(error.exception),'download_http_403')
        check_download_status(200)
    def test_rating_single_fence_with_prose_but_not_second_object(self):
        self.assertEqual(parse_response('```json\n{"events": []}\n```\nExplanation.'),{'events':[]})
        with self.assertRaises(ValueError):parse_response('```json\n{"events": []}\n```\n{"events":[1]}')
    def test_rating_plain_json_with_prose_preserves_values_and_rejects_competing_output(self):
        original = {'events': [], 'warnings': ['Needs human review']}
        import json
        self.assertEqual(parse_response(json.dumps(original) + '\n\n**Note**: Human review required.'), original)
        for suffix in ('{}', '[]', '```json\n{}\n```'):
            with self.assertRaises(ValueError):parse_response(json.dumps(original) + suffix)
        with self.assertRaises(ValueError):parse_response('{"events": [}\nNote')
    def test_resume_skips_completed_searches_and_prioritizes_unvisited(self):
        class Adapter:
            def __init__(self):self.queries=[]
            def search(self,query):
                self.queries.append(query)
                yield {'total':0,'pageItems':[]}
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(tmp);adapter=Adapter()
            issuers=[{'id':1,'name':'A','monitoring_status':'active'},{'id':2,'name':'B','monitoring_status':'active'}]
            collect(store,adapter,issuers[:1],kinds=('rating',))
            adapter.queries=[]
            collect(store,adapter,issuers,kinds=('rating',),resume=True)
            self.assertEqual(len(adapter.queries),2)
            self.assertTrue(all(q.startswith('B ') for q in adapter.queries))
            store.db.close()

    def test_source_boundary(self):
        for url in ('https://evil.example/a.pdf', '//evil.example/a.pdf',
                    'http://www.chinamoney.com.cn/a.pdf', 'https://www.chinamoney.com.cn@evil.example/a.pdf'):
            with self.assertRaises(ValueError): allowed_url(url)
        self.assertEqual(allowed_url('/a.pdf'), 'https://www.chinamoney.com.cn/a.pdf')

    def test_aggregation_is_not_empty_success(self):
        with self.assertRaises(ValueError):
            parse_result({'head': {'rep_code': '200'}, 'data': {'nodes': []}}, 1)

    def test_pause_and_failed_search(self):
        class Adapter:
            def search(self, query): raise TimeoutError('signed_url_must_not_be_logged')
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(tmp)
            with store.lock():
                summary = collect(store, Adapter(), [
                    {'id': 1, 'name': 'A', 'monitoring_status': 'active'},
                    {'id': 2, 'name': 'B', 'monitoring_status': 'paused'}], kinds=('rating',))
            self.assertEqual(summary['failed'], 2)
            self.assertEqual(summary['skipped_paused'], 1)
            self.assertEqual(store.db.execute('SELECT status FROM runs').fetchone()[0], 'partial')
            self.assertNotIn('signed_url', str([tuple(r) for r in store.db.execute('SELECT * FROM searches')]))
            store.db.close()

    def test_pdf_replay_and_changed_content(self):
        import pymupdf
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(tmp)
            item = dict(source_id='1', url=allowed_url('/a.pdf'), title='Example')
            pdf = pymupdf.open(); pdf.new_page(); one = pdf.tobytes(); pdf.new_page(); two = pdf.tobytes(); pdf.close()
            first, created = store.archive(1, 'rating', item, one)
            self.assertTrue(created)
            self.assertEqual(store.archive(1, 'rating', item, one), (first, False))
            self.assertTrue(store.archive(1, 'rating', item, two)[1])
            with self.assertRaises(ValueError): store.archive(1, 'rating', item, b'<html>blocked</html>')
            self.assertEqual(store.db.execute('SELECT COUNT(*) FROM documents').fetchone()[0], 2)
            store.db.close()

    def test_one_bad_download_does_not_hide_later_reports(self):
        import pymupdf
        pdf=pymupdf.open();pdf.new_page();content=pdf.tobytes();pdf.close()
        class Adapter:
            def search(self, query):
                yield {'total':2,'pageItems':[
                    {'id':1,'title':'broken','paths':['/bad.pdf']},
                    {'id':2,'title':'good','paths':['/good.pdf']}]}
            def download(self,item):
                if item['source_id']==1:raise ValueError('broken')
                return content
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(tmp)
            result=collect(store,Adapter(),[{'id':1,'name':'A','monitoring_status':'active'}],kinds=('rating',))
            self.assertEqual(result['downloaded'],1)
            self.assertEqual(result['failed'],2)
            self.assertEqual(store.db.execute('SELECT COUNT(*) FROM download_failures').fetchone()[0],1)
            store.db.close()

    def test_pdf_preparation_on_runtime_version(self):
        import pymupdf
        from prepare_pilot import inspect_pdf
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);pdf=pymupdf.open();page=pdf.new_page();page.insert_text((50,50),'Issuer report 2025 assets 100')
            source=root/'report.pdf';pdf.save(source);pdf.close()
            result=inspect_pdf(source,root/'prepared',None)
            self.assertEqual(result['page_count'],1)
            self.assertEqual(result['ocr_pages'],[])
            self.assertTrue((root/'prepared'/(result['sha256']+'.json')).is_file())

    def test_rating_requires_subject_and_date_evidence(self):
        self.assertEqual(chinese_dates('二〇二六年五月十二日'),['2026-05-12'])
        event = dict(issuer_name='测试银行', agency='测试评级', rating_date='2025-06-20', grade='AAA',
                     outlook='稳定', action='follow_up', subject_type='issuer')
        quotes = {'issuer_name':'测试银行', 'agency':'测试评级', 'rating_date':'2025年6月20日',
                  'grade':'主体信用等级AAA', 'outlook':'展望稳定', 'action':'跟踪评级'}
        event['evidence'] = {k:dict(page=1,quote=v) for k,v in quotes.items()}
        pages = {1:' '.join(quotes.values())}
        self.assertEqual(validate_event(event,pages,'测试银行'), [])
        event['subject_type']='bond'
        self.assertIn('not_issuer_rating',validate_event(event,pages,'测试银行'))
        event['rating_date']='2024-06-20'
        self.assertIn('date_not_in_quote',validate_event(event,pages,'测试银行'))
        event['action']='withdrawn'
        self.assertIn('withdrawn_rating_has_grade',validate_event(event,pages,'测试银行'))


if __name__ == '__main__': unittest.main()
