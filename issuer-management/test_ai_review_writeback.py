import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock,patch
import ai_review_writeback as wb

DOC='a'*64


def run_dir(folder,complete=True,observation=11):
    root=Path(folder)/'full-test-v9';(root/'results').mkdir(parents=True);(root/'documents').mkdir()
    (root/'verification.json').write_text(json.dumps({'review_complete':complete,'business_writes':False}))
    metrics=[{'MetricCode':'total_assets','ObservationId':11},{'MetricCode':'net_profit','ObservationId':12}]
    (root/'snapshot.json').write_text(json.dumps({'version':'issuer-batch-review-v9','records':[
        {'FinancialRecordId':5,'IssuerId':3,'metrics':metrics,'evidence_pages':{'7':[2]}}]}))
    (root/'documents/7.json').write_text(json.dumps({'DocumentId':7,'Sha256':DOC.upper()}))
    (root/'results/5.json').write_text(json.dumps({'results':[
        {'record_id':5,'metric':'total_assets','observation_id':observation,'status':'checks_passed'},
        {'record_id':5,'metric':'net_profit','observation_id':12,'status':'exception'}]}))
    return root


def live_metric(**changes):
    return dict({'FinancialMetricId':90,'FinancialRecordId':5,'IssuerId':3,'MetricCode':'total_assets','ObservationId':11,
                 'ValueStatus':'reported','ReviewStatus':'pending','ReviewNote':'{"scope_defaulted": true}',
                 'ReviewedBy':None,'ReviewedAt':None},**changes)


DOCS={7:{'DocumentStatus':'active','ArchiveStatus':'archived','Sha256':DOC.upper()}}


class WritebackTests(unittest.TestCase):
    def test_only_passed_results_become_candidates_with_frozen_document_hashes(self):
        with tempfile.TemporaryDirectory() as folder:
            _,version,items=wb.candidates(run_dir(folder))
        self.assertEqual(version,'issuer-batch-review-v9')
        self.assertEqual([(i['record_id'],i['metric'],i['documents']) for i in items],[(5,'total_assets',{7:DOC})])

    def test_incomplete_run_or_snapshot_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ValueError):wb.candidates(run_dir(folder,complete=False))
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ValueError):wb.candidates(run_dir(folder,observation=99))

    def test_any_live_change_keeps_metric_pending(self):
        item={'record_id':5,'metric':'total_assets','observation_id':11,'documents':{7:DOC}}
        self.assertEqual(wb.classify(item,{5:(5,'draft')},{(5,'total_assets'):live_metric()},DOCS),(None,5))
        self.assertEqual(wb.classify(item,{},{},DOCS),('record_withdrawn',None))
        cases=[('published',live_metric(),DOCS,'record_not_draft'),('draft',None,DOCS,'metric_missing'),
               ('draft',live_metric(ObservationId=12),DOCS,'observation_changed'),
               ('draft',live_metric(ReviewStatus='verified'),DOCS,'already_reviewed'),
               ('draft',live_metric(ValueStatus='unresolved'),DOCS,'not_reported'),
               ('draft',live_metric(),{7:dict(DOCS[7],Sha256='B'*64)},'document_changed'),
               ('draft',live_metric(),{7:dict(DOCS[7],DocumentStatus='withdrawn')},'document_changed'),
               ('draft',live_metric(),{},'document_changed')]
        for status,metric,docs,reason in cases:
            self.assertEqual(wb.check(item,status,metric,docs),reason)

    def test_newer_draft_of_same_period_is_marked_only_if_it_keeps_the_reviewed_observation(self):
        item={'record_id':5,'metric':'total_assets','observation_id':11,'documents':{7:DOC}}
        current={5:(8,'draft')}
        self.assertEqual(wb.classify(item,current,{(8,'total_assets'):live_metric(FinancialRecordId=8)},DOCS),(None,8))
        self.assertEqual(wb.classify(item,current,{(8,'total_assets'):live_metric(FinancialRecordId=8,ObservationId=13),
                                                   (5,'total_assets'):live_metric()},DOCS),('observation_changed',8))

    def test_live_state_maps_each_record_to_newest_of_its_period(self):
        rows=[{'FinancialRecordId':5,'IssuerId':3,'PeriodEnd':'2025-06-30','RecordStatus':'draft'},
              {'FinancialRecordId':8,'IssuerId':3,'PeriodEnd':'2025-06-30','RecordStatus':'draft'},
              {'FinancialRecordId':6,'IssuerId':3,'PeriodEnd':'2025-12-31','RecordStatus':'draft'}]
        with patch.object(wb,'query',side_effect=[rows,[],[]]):
            current,_,_=wb.live_state(MagicMock())
        self.assertEqual(current,{5:(8,'draft'),8:(8,'draft'),6:(6,'draft')})

    def test_note_keeps_existing_fields_and_wraps_plain_text(self):
        provenance={'run':'r'}
        self.assertEqual(json.loads(wb.reviewed_note('{"scope_defaulted": true}',provenance)),{'scope_defaulted':True,'ai_review':provenance})
        self.assertEqual(json.loads(wb.reviewed_note('legacy text',provenance)),{'previous_note':'legacy text','ai_review':provenance})
        self.assertEqual(json.loads(wb.reviewed_note(None,provenance)),{'ai_review':provenance})

    def test_apply_updates_pending_rows_logs_and_commits_after_receipt(self):
        cn=MagicMock();cn.execute.return_value.rowcount=1;cn.execute.return_value.fetchval.return_value=0
        with tempfile.TemporaryDirectory() as folder:
            root=run_dir(folder);out=Path(folder)/'out'
            with patch.object(wb,'live_state',return_value=({5:(5,'draft')},{(5,'total_assets'):live_metric()},DOCS)):
                receipt=wb.writeback(cn,root,out,True)
            saved=json.loads((out/'writeback-applied.json').read_text())
        sql=[c.args[0] for c in cn.execute.call_args_list]
        self.assertEqual(sum('sp_getapplock' in s for s in sql),1)
        update=next(c for c in cn.execute.call_args_list if 'UPDATE DV.IssuerFinancialMetric' in c.args[0])
        self.assertIn("ReviewStatus='pending'",update.args[0])
        self.assertEqual(update.args[1],'ai-review:full-test-v9');self.assertEqual(update.args[3:],(90,11))
        log=next(c for c in cn.execute.call_args_list if 'IssuerChangeLog' in c.args[0])
        self.assertEqual(log.args[2],'ai_verify_metric');self.assertEqual(log.args[7],'full-test-v9:5:total_assets')
        cn.commit.assert_called_once();cn.rollback.assert_not_called()
        self.assertEqual((receipt['verified'],saved['committed']),(1,True))
        self.assertEqual(saved['eligible'][0]['before']['ReviewStatus'],'pending')

    def test_apply_aborts_when_row_changes_during_write(self):
        cn=MagicMock();cn.execute.return_value.rowcount=0;cn.execute.return_value.fetchval.return_value=0
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(wb,'live_state',return_value=({5:(5,'draft')},{(5,'total_assets'):live_metric()},DOCS)):
                with self.assertRaises(RuntimeError):wb.writeback(cn,run_dir(folder),Path(folder)/'out',True)
        cn.commit.assert_not_called()

    def test_dry_run_writes_nothing(self):
        cn=MagicMock()
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(wb,'live_state',return_value=({5:(5,'draft')},{(5,'total_assets'):live_metric(ReviewStatus='verified')},DOCS)):
                receipt=wb.writeback(cn,run_dir(folder),Path(folder)/'out',False)
        self.assertEqual((receipt['verified'],receipt['skipped']),(0,{'already_reviewed':1}))
        self.assertFalse(any('UPDATE' in c.args[0] or 'INSERT' in c.args[0] for c in cn.execute.call_args_list))
        cn.commit.assert_not_called()

    def test_revert_restores_prior_row_only_for_this_actor(self):
        cn=MagicMock();cn.execute.return_value.rowcount=1;cn.execute.return_value.fetchval.return_value=0
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'receipt.json'
            path.write_text(json.dumps({'mode':'applied','run':'r','actor':'ai-review:v9','eligible':[
                {'issuer_id':3,'record_id':5,'target_record_id':5,'metric':'total_assets','before':live_metric()}]}))
            self.assertEqual(wb.revert(cn,path),1)
        update=next(c for c in cn.execute.call_args_list if 'UPDATE DV.IssuerFinancialMetric' in c.args[0])
        self.assertEqual(update.args[1:],('pending',None,None,'{"scope_defaulted": true}',90,11,'ai-review:v9'))
        cn.commit.assert_called_once()


if __name__=='__main__':unittest.main()
