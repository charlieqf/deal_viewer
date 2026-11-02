"""Build reviewable draft selections and export a read-back from SQL, never auto-publish."""
import json
from collections import defaultdict
from decimal import Decimal

from pilot_runtime import ARTIFACTS,METRICS,DATABASE,atomic_json,encode,sha,guarded_connection

POLICY={'version':'spdb-pilot-selection-v1','profile':'group_pilot','review':'pending',
        'priority':['native_text','paddleocr'],'scope':['accounting_consolidated','regulatory_consolidated'],
        'warning':'Automatic draft only; accounting basis and OCR require review.'}


def query(cn,sql,*args):
    cur=cn.execute(sql,*args);cols=[x[0] for x in cur.description]
    return [dict(zip(cols,row)) for row in cur.fetchall()]


def main():
    cn=guarded_connection();by_period=defaultdict(list)
    candidates=query(cn,"""SELECT o.*,e.DocumentId,e.PdfPage,e.RawValue,e.RawUnit,e.Method,d.Title
      FROM DV.IssuerMetricObservation o JOIN DV.IssuerObservationEvidence e ON e.ObservationId=o.ObservationId AND e.EvidenceRole='value'
      JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId
      WHERE o.EntityScopeCode IN ('accounting_consolidated','regulatory_consolidated') ORDER BY o.ObservationId""")
    for c in candidates:by_period[c['PeriodEnd']].append(c)
    created=0
    for period,values in sorted(by_period.items()):
        issuer=values[0]['IssuerId'];selected={};notes={}
        for code in METRICS:
            pool=[v for v in values if v['MetricCode']==code]
            pool.sort(key=lambda v:(v['Method']!='native_text','审计' in v['Title'],v['AccountingBasisCode']=='unknown',v['ObservationId']))
            if pool:
                chosen=pool[0];selected[code]=chosen
                same=[v for v in pool if all(v[k]==chosen[k] for k in ('EntityScopeCode','AccountingBasisCode','CurrencyCode','MeasurementBasisCode','PeriodStart','PeriodBasis'))]
                conflicts=sorted({str(v['NumericValue']) for v in same})
                notes[code]={'candidate_count':len(pool),'comparable_values':conflicts,'conflict':len(conflicts)>1,
                             'issues':(['accounting_basis_not_stated'] if chosen['AccountingBasisCode']=='unknown' else [])+
                             (['ratio_accounting_basis_requires_review'] if code in METRICS[1:4] and chosen['AccountingBasisCode']!='not_applicable' else [])+
                             (['ocr_requires_visual_review'] if chosen['Method']=='paddleocr' else []),
                             'selected_attempt':chosen['AttemptId'],'source_page':chosen['PdfPage']}
        amount_bases={selected[c]['AccountingBasisCode'] for c in ('total_assets','operating_revenue','total_profit') if c in selected}
        basis=next(iter(amount_bases)) if len(amount_bases)==1 else 'unknown'
        start=period.replace(month=1,day=1)
        series=cn.execute("SELECT SeriesId FROM DV.IssuerFinancialSeries WITH(UPDLOCK,HOLDLOCK) WHERE IssuerId=? AND PeriodStart=? AND PeriodEnd=? AND FlowBasis='year_to_date' AND PresentationScopeCode='consolidated' AND AccountingBasisCode=? AND CurrencyCode='CNY' AND SelectionProfileCode='group_pilot'",issuer,start,period,basis).fetchval()
        if not series:
            series=cn.execute("INSERT DV.IssuerFinancialSeries(IssuerId,PeriodStart,PeriodEnd,FlowBasis,PresentationScopeCode,AccountingBasisCode,CurrencyCode,SelectionProfileCode) OUTPUT inserted.SeriesId VALUES(?,?,?,'year_to_date','consolidated',?,'CNY','group_pilot')",issuer,start,period,basis).fetchval()
        record=cn.execute('SELECT FinancialRecordId FROM DV.IssuerFinancialRecord WHERE SeriesId=? AND VersionNo=1',series).fetchval()
        if record:cn.commit();continue
        primary=selected.get('total_assets',next(iter(selected.values())))['DocumentId']
        record=cn.execute("INSERT DV.IssuerFinancialRecord(SeriesId,IssuerId,VersionNo,SelectionPolicyVersion,SelectionPolicyHash,PrimaryDocumentId) OUTPUT inserted.FinancialRecordId VALUES(?,?,1,?,?,?)",series,issuer,POLICY['version'],sha(encode(POLICY)),primary).fetchval()
        for code in METRICS:
            selected_value=selected.get(code);note=notes.get(code,{'issues':['no_valid_candidate_in_supplied_pages']})
            cn.execute("INSERT DV.IssuerFinancialMetric(FinancialRecordId,IssuerId,MetricCode,ObservationId,ValueStatus,ReviewStatus,ReviewNote) VALUES(?,?,?,?,?,'pending',?)",record,issuer,code,selected_value['ObservationId'] if selected_value else None,'reported' if selected_value else 'unresolved',json.dumps(note,ensure_ascii=False))
        cn.execute('UPDATE DV.IssuerFinancialSeries SET NextVersionNo=2 WHERE SeriesId=?',series)
        cn.execute("INSERT DV.IssuerChangeLog(EntityType,EntityId,Action,AfterJson,Reason,ActorId,RequestId,RequestHash) VALUES('record',?,'create_draft',?,N'真实材料自动初选；尚未完成业务复核','codex-pilot',?,?)",record,json.dumps({'policy':POLICY,'period_end':str(period)},ensure_ascii=False),'pilot-draft-'+str(record),sha(encode(POLICY)))
        cn.commit();created+=1
    result=export(cn);cn.close()
    atomic_json(ARTIFACTS/'sql-readback.json',result)
    print(json.dumps({'created_drafts':created,'counts':result['counts'],'periods':len(result['records'])},ensure_ascii=False),flush=True)


def export(cn,issuer_id=None,root_task_id=None,record_ids=None):
    scoped=issuer_id is not None
    if scoped and (root_task_id is None or not record_ids):raise ValueError('Scoped export requires issuer, root and selected records')
    if not scoped and (root_task_id is not None or record_ids is not None):raise ValueError('Partial export scope is not allowed')
    ids=list(map(int,record_ids or []));marks=','.join('?' for _ in ids)
    record_filter=(' WHERE r.IssuerId=? AND r.FinancialRecordId IN ('+marks+')') if scoped else ''
    metric_filter=(' WHERE m.IssuerId=? AND m.FinancialRecordId IN ('+marks+')') if scoped else ''
    record_args=[issuer_id,*ids] if scoped else []
    tables=['Issuer','IssuerFile','IssuerDocument','IssuerDocumentSource','IssuerMetricObservation','IssuerObservationEvidence','IssuerFinancialSeries','IssuerFinancialRecord','IssuerFinancialMetric','IssuerProcessingTask','IssuerProcessingAttempt']
    counts={t:cn.execute('SELECT COUNT(*) FROM DV.'+t).fetchval() for t in tables}
    records=query(cn,"""SELECT r.FinancialRecordId,r.SeriesId,r.VersionNo,r.RecordStatus,s.PeriodStart,s.PeriodEnd,s.AccountingBasisCode,s.SelectionProfileCode,s.CurrentRecordId
      FROM DV.IssuerFinancialRecord r JOIN DV.IssuerFinancialSeries s ON r.SeriesId=s.SeriesId"""+record_filter+" ORDER BY s.PeriodEnd DESC,r.VersionNo DESC",*record_args)
    metrics=query(cn,"""SELECT m.FinancialRecordId,m.MetricCode,m.ValueStatus,m.MissingReason,m.ReviewStatus,m.ReviewNote,o.ObservationId,o.NumericValue,o.UnitCode,o.EntityScopeCode,o.AccountingBasisCode,o.MeasurementBasisCode,o.MeasurementBasisLabel,o.PeriodStart,o.PeriodEnd,o.AttemptId
      FROM DV.IssuerFinancialMetric m LEFT JOIN DV.IssuerMetricObservation o ON o.ObservationId=m.ObservationId"""+metric_filter+" ORDER BY m.FinancialRecordId,m.MetricCode",*record_args)
    evidence=query(cn,"""SELECT e.ObservationId,e.DocumentId,e.PdfPage,e.EvidenceRole,e.RawValue,e.RawUnit,e.ColumnHeader,e.Method,e.RawText,d.Title,f.StorageKey,f.StorageBackendCode,f.ArchiveStatus,CONVERT(varchar(64),f.Sha256,2) Sha256
      FROM DV.IssuerObservationEvidence e JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId JOIN DV.IssuerFile f ON f.FileId=d.FileId"""+
      (" WHERE e.IssuerId=? AND EXISTS(SELECT 1 FROM DV.IssuerFinancialMetric m WHERE m.ObservationId=e.ObservationId AND m.FinancialRecordId IN ("+marks+"))" if scoped else '')+" ORDER BY e.ObservationId,e.EvidenceId",*record_args)
    attempts=query(cn,'SELECT AttemptId,TaskId,AttemptNo,LeaseEpoch,Engine,Model,Status,InputTokens,OutputTokens,UsageStatus,ErrorCode FROM DV.IssuerProcessingAttempt'+(' WHERE TaskId IN (SELECT TaskId FROM DV.IssuerProcessingTask WHERE IssuerId=? AND RootTaskId=?)' if scoped else '')+' ORDER BY AttemptId',*([issuer_id,root_task_id] if scoped else []))
    tasks=query(cn,'SELECT TaskId,Stage,Status,OutcomeCode,LeaseEpoch FROM DV.IssuerProcessingTask'+(' WHERE IssuerId=? AND (RootTaskId=? OR TaskId=?)' if scoped else '')+' ORDER BY TaskId',*([issuer_id,root_task_id,root_task_id] if scoped else []))
    files=query(cn,'SELECT d.DocumentId,d.Title,d.MainPeriodStart,d.MainPeriodEnd,f.PageCount,f.ByteSize,f.StorageKey,f.StorageBackendCode,f.ArchiveStatus,f.ArchivedAt,CONVERT(varchar(64),f.Sha256,2) Sha256 FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId'+(' WHERE d.IssuerId=?' if scoped else '')+' ORDER BY d.MainPeriodEnd,d.Title',*([issuer_id] if scoped else []))
    # Explicit string contract for SQL bigint/decimal, including nested metric IDs.
    def serial(value,key=''):
        if isinstance(value,dict):return {k:serial(v,k) for k,v in value.items()}
        if isinstance(value,list):return [serial(v) for v in value]
        if value is None:return None
        if isinstance(value,Decimal) or (isinstance(value,int) and key.endswith('Id')):return str(value)
        if hasattr(value,'isoformat'):return value.isoformat()
        return value
    return serial({'database':DATABASE,'exported_at_utc':cn.execute('SELECT SYSUTCDATETIME()').fetchval(),
                   'counts':counts,'records':records,'metrics':metrics,'evidence':evidence,'attempts':attempts,'tasks':tasks,'files':files,
                   'published_count':cn.execute('SELECT COUNT(*) FROM DV.vw_IssuerFinancialPublished').fetchval(),'policy':POLICY})


if __name__=='__main__':main()
