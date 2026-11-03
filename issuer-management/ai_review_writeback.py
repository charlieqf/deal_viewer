"""Mark draft metrics that passed the dual-model AI review as verified.

Dry run unless --apply. Everything is rechecked against live SQL inside the write
transaction; anything changed since the frozen review stays pending for people.
--revert restores the exact prior rows from an applied receipt.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from ai_review import MODELS,query,save

ACTION='ai_verify_metric'
RECORDS="""SELECT r.FinancialRecordId,r.IssuerId,s.PeriodEnd,r.RecordStatus FROM DV.IssuerFinancialRecord r
  JOIN DV.IssuerFinancialSeries s ON s.SeriesId=r.SeriesId JOIN DV.Issuer i ON i.IssuerId=r.IssuerId
  WHERE r.RecordStatus<>'withdrawn' AND i.MonitoringStatus='active'"""


def encode(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,default=str)


def candidates(root):
    verification=json.loads((root/'verification.json').read_text(encoding='utf-8'))
    if not verification.get('review_complete') or verification.get('business_writes'):
        raise ValueError('AI review run is incomplete or already wrote business data')
    snapshot=json.loads((root/'snapshot.json').read_text(encoding='utf-8'));records={r['FinancialRecordId']:r for r in snapshot['records']}
    hashes={}
    def document_hash(id_):
        if id_ not in hashes:hashes[id_]=json.loads((root/'documents'/f'{id_}.json').read_text(encoding='utf-8'))['Sha256'].lower()
        return hashes[id_]
    items=[]
    for path in sorted((root/'results').glob('*.json')):
        for result in json.loads(path.read_text(encoding='utf-8'))['results']:
            if result['status']!='checks_passed':continue
            record=records[result['record_id']]
            stored=next(m for m in record['metrics'] if m['MetricCode']==result['metric'])
            if stored['ObservationId']!=result['observation_id']:raise ValueError('Result does not match the frozen snapshot')
            items.append({'issuer_id':record['IssuerId'],'record_id':result['record_id'],'metric':result['metric'],
                          'observation_id':result['observation_id'],'result_file':path.name,
                          'documents':{int(d):document_hash(int(d)) for d in record['evidence_pages']}})
    return verification,snapshot.get('version'),items


def classify(item,current,metrics,documents):
    """Return (reason, displayed record). Reason is None only when that record still points at the reviewed observation."""
    if item['record_id'] not in current:return 'record_withdrawn',None
    target,status=current[item['record_id']]
    return check(item,status,metrics.get((target,item['metric'])),documents),target


def check(item,status,metric,documents):
    # A newer draft of the same period may reuse the reviewed observation; observations are immutable.
    if status!='draft':return 'record_not_draft'
    if not metric:return 'metric_missing'
    if metric['ObservationId']!=item['observation_id']:return 'observation_changed'
    if metric['ValueStatus']!='reported':return 'not_reported'
    if metric['ReviewStatus']!='pending':return 'already_reviewed'
    for id_,digest in item['documents'].items():
        live=documents.get(id_)
        if not live or live['DocumentStatus']!='active' or live['ArchiveStatus']!='archived' or live['Sha256'].lower()!=digest:
            return 'document_changed'
    return None


def reviewed_note(raw,provenance):
    try:note=json.loads(raw) if raw else {}
    except ValueError:note=None
    if not isinstance(note,dict):note={'previous_note':raw}
    return encode(dict(note,ai_review=provenance))


def live_state(cn):
    rows=query(cn,RECORDS);newest={}
    for r in rows:
        key=(r['IssuerId'],r['PeriodEnd'])
        if key not in newest or r['FinancialRecordId']>newest[key]['FinancialRecordId']:newest[key]=r
    # Every live record maps to the newest record of its issuer/period, which is the one the page shows.
    current={r['FinancialRecordId']:(newest[(r['IssuerId'],r['PeriodEnd'])]['FinancialRecordId'],
                                     newest[(r['IssuerId'],r['PeriodEnd'])]['RecordStatus']) for r in rows}
    metrics={(m['FinancialRecordId'],m['MetricCode']):m for m in query(cn,"""SELECT FinancialMetricId,FinancialRecordId,IssuerId,
      MetricCode,ObservationId,ValueStatus,ReviewStatus,ReviewNote,ReviewedBy,ReviewedAt FROM DV.IssuerFinancialMetric""")}
    documents={d['DocumentId']:d for d in query(cn,"""SELECT d.DocumentId,d.DocumentStatus,f.ArchiveStatus,
      CONVERT(varchar(64),f.Sha256,2) Sha256 FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId""")}
    return current,metrics,documents


def lock_issuers(cn,issuers):
    # Same resource as manual review, so a person cannot save the same issuer mid-write.
    for issuer in sorted(issuers):
        got=cn.execute("""DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=?,@LockMode='Exclusive',
          @LockOwner='Transaction',@LockTimeout=10000; SELECT @r""",'issuer-review-'+str(issuer)).fetchval()
        if got<0:raise RuntimeError('Issuer review lock unavailable: '+str(issuer))


def writeback(cn,root,out,apply):
    verification,version,items=candidates(root)
    # The snapshot keeps the version that froze it; the run name identifies the reviewer that finished it.
    run=root.name;actor='ai-review:'+run
    release=root/'git-release.json'
    provenance={'run':run,'snapshot_version':version,'models':list(MODELS),'result':'checks_passed',
                'reviewer_commit':json.loads(release.read_text()).get('commit') if release.exists() else None,
                'verification_sha256':hashlib.sha256((root/'verification.json').read_bytes()).hexdigest()}
    # A rule-based run (e.g. the trend rule) names its rule and reason instead of the dual-model wording.
    if verification.get('rule'):provenance['rule']=verification['rule']
    reason=(verification.get('reason') or 'AI双模型独立复核通过')+'（'+run+'）'
    if apply:lock_issuers(cn,{i['issuer_id'] for i in items})
    current,metrics,documents=live_state(cn)
    eligible,skipped=[],[]
    for item in items:
        skip,target=classify(item,current,metrics,documents)
        if skip:skipped.append(dict(item,reason=skip,target_record_id=target))
        else:eligible.append(dict(item,target_record_id=target,before=metrics[(target,item['metric'])]))
    if apply:
        for item in eligible:
            before=item['before'];note=reviewed_note(before['ReviewNote'],provenance)
            changed=cn.execute("""UPDATE DV.IssuerFinancialMetric SET ReviewStatus='verified',ReviewedBy=?,ReviewedAt=SYSUTCDATETIME(),
              ReviewNote=? WHERE FinancialMetricId=? AND ObservationId=? AND ReviewStatus='pending'""",
              actor,note,before['FinancialMetricId'],before['ObservationId']).rowcount
            if changed!=1:raise RuntimeError('Metric changed during write: '+str(before['FinancialMetricId']))
            after=encode({'record_id':item['target_record_id'],'reviewed_record_id':item['record_id'],'metric':item['metric'],
                          'observation_id':item['observation_id'],'review_status':'verified','reviewed_by':actor,'ai_review':provenance})
            cn.execute("""INSERT DV.IssuerChangeLog(EntityType,EntityId,Action,BeforeJson,AfterJson,Reason,ActorId,RequestId,RequestHash)
              VALUES('issuer',?,?,?,?,?,?,?,?)""",item['issuer_id'],ACTION,encode(before),after,
              reason,actor,f"{run}:{item['target_record_id']}:{item['metric']}",
              hashlib.sha256(after.encode('utf-8')).digest())
    receipt={'mode':'applied' if apply else 'dry_run','run':run,'version':version,'actor':actor,
             'at':datetime.now(timezone.utc).isoformat(),'checks_passed':len(items),'verified':len(eligible),
             'skipped':dict(Counter(s['reason'] for s in skipped)),'eligible':eligible,'skipped_items':skipped}
    # Persist the receipt before commit: it holds the prior rows needed for --revert.
    path=out/('writeback-applied.json' if apply else 'writeback-dry-run.json');save(path,receipt)
    if apply:
        cn.commit();receipt['committed']=True;save(path,receipt)
    else:cn.rollback()
    return receipt


def revert(cn,receipt_path):
    receipt=json.loads(receipt_path.read_text())
    if receipt['mode']!='applied':raise ValueError('Only an applied receipt can be reverted')
    lock_issuers(cn,{i['issuer_id'] for i in receipt['eligible']})
    for item in receipt['eligible']:
        before=item['before']
        changed=cn.execute("""UPDATE DV.IssuerFinancialMetric SET ReviewStatus=?,ReviewedBy=?,ReviewedAt=?,ReviewNote=?
          WHERE FinancialMetricId=? AND ObservationId=? AND ReviewStatus='verified' AND ReviewedBy=?""",
          before['ReviewStatus'],before['ReviewedBy'],before['ReviewedAt'],before['ReviewNote'],
          before['FinancialMetricId'],before['ObservationId'],receipt['actor']).rowcount
        if changed!=1:raise RuntimeError('Metric changed after write-back: '+str(before['FinancialMetricId']))
        cn.execute("""INSERT DV.IssuerChangeLog(EntityType,EntityId,Action,BeforeJson,AfterJson,Reason,ActorId,RequestId)
          VALUES('issuer',?,'ai_verify_revert',NULL,?,N'撤销AI复核写回',?,?)""",item['issuer_id'],encode(before),
          receipt['actor'],f"{receipt['run']}:{item['target_record_id']}:{item['metric']}")
    cn.commit()
    return len(receipt['eligible'])


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',type=Path,help='Completed AI review run directory (read only)')
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--revert',type=Path,help='Applied receipt to undo')
    args=parser.parse_args()
    from pilot_runtime import PRODUCTION,guarded_connection
    if not PRODUCTION:raise RuntimeError('Write-back targets the production issuer database only')
    cn=guarded_connection()
    try:
        if args.revert:print(json.dumps({'reverted':revert(cn,args.revert)}));return
        receipt=writeback(cn,args.root,args.out,args.apply)
        print(json.dumps({k:receipt[k] for k in ('mode','run','checks_passed','verified','skipped')},ensure_ascii=False))
    except Exception:
        cn.rollback();raise
    finally:cn.close()


if __name__=='__main__':main()
