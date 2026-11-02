"""Append evidence-checked disclosures; scope remains unknown and business review pending."""
import argparse
import gzip
import json
from pathlib import Path
from pilot_runtime import ROOT,ARTIFACTS,ISSUER,atomic_json,encode,sha,guarded_connection
from pilot_v2_prepare import merged_pages
from pilot_v2_validation import validate
from disclosure_review import reviewed_candidate

ENGINE='document_disclosure_review'

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--manifest',required=True);args=parser.parse_args()
    manifest=json.loads(Path(args.manifest).read_text(encoding='utf-8'))
    assert manifest['issuer']==ISSUER
    docs={d['sha256']:d for d in json.loads((ARTIFACTS/'registered.json').read_text())}
    bundle=json.loads((ROOT/'normalized-bundle.json').read_text())
    cn=guarded_connection();added=[]
    issuer=next(iter(docs.values()))['issuer_id']
    assert cn.execute('SELECT MonitoringStatus FROM DV.Issuer WHERE IssuerId=?',issuer).fetchval()=='active'
    source_hash=sha(Path(__file__).read_bytes()+Path(__file__).with_name('disclosure_review.py').read_bytes()).hex()
    for item in manifest['items']:
        doc=docs[item['sha256']]
        assert doc['document_id']==item['document_id'] and doc['issuer_id']==issuer
        assert item['visual_checked'] is True
        pages=merged_pages(doc);page=dict(pages[item['page']])
        c=reviewed_candidate(item,doc,pages)
        numeric,unit,_,context=validate(c,doc,list(pages.values()))
        lineage={'engine':ENGINE,'version':1,'issuer':ISSUER,'document_sha256':doc['sha256'],
                 'source_hash':source_hash,'item':item,'candidate':c,'page':page,
                 'business_review':'pending','scope_review':c['entity_scope']}
        digest=sha(encode(lineage));key=sha(b'disclosure-review-v1:'+digest)
        task=cn.execute('SELECT TaskId FROM DV.IssuerProcessingTask WITH(UPDLOCK,HOLDLOCK) WHERE IdempotencyKey=?',key).fetchval()
        input_path=ARTIFACTS/'disclosure-inputs'/(digest.hex()+'.json');atomic_json(input_path,lineage)
        if task:
            attempt=cn.execute('SELECT AttemptId FROM DV.IssuerProcessingAttempt WHERE TaskId=? AND AttemptNo=1',task).fetchval()
            if any(a['attempt_id']==attempt for a in bundle['attempts']):cn.rollback();continue
        else:
            existing=cn.execute("""SELECT TOP 1 m.ValueStatus FROM DV.IssuerFinancialMetric m
                JOIN DV.IssuerFinancialRecord r ON r.FinancialRecordId=m.FinancialRecordId
                JOIN DV.IssuerFinancialSeries s ON s.SeriesId=r.SeriesId
                WHERE r.IssuerId=? AND s.PeriodEnd=? AND m.MetricCode=? AND r.RecordStatus<>'withdrawn'
                ORDER BY r.FinancialRecordId DESC""",issuer,item['period_end'],item['metric_code']).fetchval()
            assert existing=='unresolved','Do not replace an already selected value'
            task=cn.execute("""INSERT DV.IssuerProcessingTask(RootTaskId,ParentTaskId,IssuerId,DocumentId,Stage,
                InputManifestKey,InputHash,IdempotencyKey,PipelineManifestHash,Status,LeaseOwner,LeaseEpoch,LeaseExpiresAt)
                OUTPUT inserted.TaskId VALUES(?,?,?,?,?,?,?,?,?,'running','disclosure-review',1,DATEADD(hour,1,SYSUTCDATETIME()))""",
                doc['root_task_id'],doc['root_task_id'],issuer,doc['document_id'],'disclosure_review',
                str(input_path.relative_to(ROOT)),digest,key,sha(b'disclosure-review-v1')).fetchval()
            attempt=cn.execute("""INSERT DV.IssuerProcessingAttempt(TaskId,AttemptNo,LeaseEpoch,Engine,SchemaVersion,
                InputHash,InputStorageKey,Status,InputTokens,OutputTokens,UsageStatus)
                OUTPUT inserted.AttemptId VALUES(?,1,1,?,'issuer-metrics-v1',?,?,'response_saved',0,0,'known')""",
                task,ENGINE,digest,str(input_path.relative_to(ROOT))).fetchval()
        result_path=ARTIFACTS/'disclosure-results'/(str(attempt)+'.json')
        atomic_json(result_path,dict(lineage,attempt_id=attempt,numeric_value=str(numeric)))
        evidence=[]
        for pn in sorted(context):
            ep=pages[pn];page_key=doc['sha256']+':'+str(pn)+':'+sha(encode(ep)).hex()[:16]
            region=None
            if ep.get('ocr_artifact'):
                ocr=json.loads((ARTIFACTS/ep['ocr_artifact']).read_text())
                region={k:ocr[k] for k in ['width','height','original_width','original_height','rotation_ccw','blocks'] if k in ocr}
            bundle['pages'][page_key]=dict(ep,region=region)
            evidence.append({'page_key':page_key,'role':'value' if pn==c['page'] else 'context'})
        candidate=dict(c,ordinal=1,numeric_value=str(numeric),unit_code=unit,
                       measurement_label='原文未注明',measurement_code='not_specified',
                       evidence=evidence)
        bundle['attempts'].append(dict(attempt_id=attempt,task_id=task,lease_epoch=1,input_hash=digest.hex(),
            sha256=doc['sha256'],result_hash=sha(result_path.read_bytes()).hex(),result_storage_key=str(result_path.relative_to(ROOT)),
            request_id=None,usage={'prompt_tokens':0,'completion_tokens':0},elapsed_seconds=0,
            metadata={'scope_review':'unconfirmed','visual_checked':True,'source_hash':source_hash},
            candidates=[candidate],validation_errors=[],engine=ENGINE,created_by='codex-evidence-review'))
        cn.commit();added.append({'attempt_id':attempt,'period':item['period_end'],'metric':item['metric_code'],'value':str(numeric)})
    cn.close();atomic_json(ROOT/'normalized-bundle.json',bundle)
    compressed=gzip.compress(encode(bundle));(ROOT/'normalized-bundle.json.gz').write_bytes(compressed)
    (ROOT/'input/normalized-bundle.json.gz').write_bytes(compressed)
    result={'added':added,'new_model_calls':0,'business_review':'pending'}
    atomic_json(ARTIFACTS/'disclosure-review.json',result);print(json.dumps(result))

if __name__=='__main__':main()
