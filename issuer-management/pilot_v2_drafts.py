"""Per-issuer, per-run draft selection with append-only versions and SQL read-back."""
from collections import defaultdict
import json
from pilot_runtime import ROOT, ARTIFACTS, CONFIG, ISSUER, METRICS, PRODUCTION, encode, sha, atomic_json, guarded_connection
from pilot_create_drafts import query, export
from pilot_v2_validation import scope_conflict,partial_profit_scope
from pilot_v2_prepare import merged_pages
from evidence_search import classify_gap
from profit_extension import load_baseline

POLICY={'version':'issuer-selection-v2','profile':'group_pilot','run_id':CONFIG.get('run_id'),
        'priority':['known_accounting_basis','native_text','main_report'],'review':'pending'}


def main():
    docs=json.loads((ARTIFACTS/'registered.json').read_text(encoding='utf-8'))
    issuer=docs[0]['issuer_id']; root=docs[0]['root_task_id']; cn=guarded_connection()
    assert all(d['issuer_id']==issuer and d['root_task_id']==root for d in docs)
    baseline,carry_ids=load_baseline(ROOT,CONFIG,ISSUER)
    if CONFIG.get('acquisition_version'):
        # Incremental reports must not erase already selected metrics from the same period.
        # Reviewer observations are restored separately below and retain their review metadata.
        carry_ids = [int(r[0]) for r in cn.execute('''WITH current_records AS (
          SELECT r.FinancialRecordId,ROW_NUMBER() OVER(PARTITION BY r.IssuerId,s.PeriodEnd ORDER BY r.FinancialRecordId DESC) n
          FROM DV.IssuerFinancialRecord r JOIN DV.IssuerFinancialSeries s ON s.SeriesId=r.SeriesId
          WHERE r.IssuerId=? AND r.RecordStatus<>'withdrawn')
          SELECT DISTINCT m.ObservationId FROM current_records r JOIN DV.IssuerFinancialMetric m
          ON m.FinancialRecordId=r.FinancialRecordId WHERE r.n=1 AND m.ObservationId IS NOT NULL''',issuer)]
    root_filter='t.RootTaskId=?'+(' OR o.ObservationId IN ('+','.join('?' for _ in carry_ids)+')' if carry_ids else '')
    bank_scope_default=PRODUCTION and cn.execute('SELECT InstitutionType FROM DV.Issuer WHERE IssuerId=?',issuer).fetchval()=='bank'
    rows=query(cn,"""SELECT o.*,a.Engine AS SourceEngine,e.DocumentId,e.PdfPage,e.RawValue,e.RawUnit,e.Method,e.RawText AS SourceText,d.Title
      FROM DV.IssuerMetricObservation o JOIN DV.IssuerProcessingAttempt a ON a.AttemptId=o.AttemptId
      JOIN DV.IssuerProcessingTask t ON t.TaskId=a.TaskId
      JOIN DV.IssuerObservationEvidence e ON e.ObservationId=o.ObservationId AND e.EvidenceRole='value'
      JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId
      WHERE o.IssuerId=? AND ("""+root_filter+""") AND (o.EntityScopeCode IN ('accounting_consolidated','regulatory_consolidated')
        OR (o.EntityScopeCode IN ('unknown','standalone') AND (a.Engine='document_disclosure_review' OR ?=1)))
      ORDER BY o.ObservationId""",issuer,root,*carry_ids,int(bank_scope_default))
    manual_rows=query(cn,"""WITH current_records AS (
      SELECT r.FinancialRecordId,ROW_NUMBER() OVER(PARTITION BY r.IssuerId,s.PeriodEnd ORDER BY r.FinancialRecordId DESC) n
      FROM DV.IssuerFinancialRecord r JOIN DV.IssuerFinancialSeries s ON s.SeriesId=r.SeriesId
      WHERE r.IssuerId=? AND r.RecordStatus<>'withdrawn')
      SELECT o.*,m.ReviewNote,m.ReviewStatus,m.ReviewedBy,m.ReviewedAt,e.DocumentId,e.PdfPage,e.RawValue,e.RawUnit,
        e.Method,e.RawText AS SourceText,d.Title,'manual_confirmation' AS SourceEngine
      FROM current_records r JOIN DV.IssuerFinancialMetric m ON m.FinancialRecordId=r.FinancialRecordId
      JOIN DV.IssuerMetricObservation o ON o.ObservationId=m.ObservationId
      JOIN DV.IssuerObservationEvidence e ON e.ObservationId=o.ObservationId AND e.EvidenceRole='value'
      JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId
      WHERE r.n=1 AND o.Origin='reviewer' AND d.DocumentStatus='active'""",issuer)
    manual={(str(v['PeriodEnd']),v['MetricCode']):v for v in manual_rows}
    periods=sorted({d['period_end'] for d in docs}); created=[]; record_ids=[]
    if baseline:assert set(periods)=={p for p,c in baseline}
    page_sets={}; gap_audit=[]
    bundle=json.loads((ROOT/'normalized-bundle.json').read_text(encoding='utf-8'))
    for period in periods:
        values=[x for x in rows if str(x['PeriodEnd'])==period]; selected={}; notes={}
        for code in METRICS:
            scope='regulatory_consolidated' if code=='capital_adequacy_ratio' else 'accounting_consolidated'
            pool=[v for v in values if v['MetricCode']==code and v['EntityScopeCode']==scope and not scope_conflict(code,scope,v['RawValue'],v['SourceText'])]
            if not pool:
                pool=[v for v in values if v['MetricCode']==code and v['EntityScopeCode'] in ('unknown','standalone')
                      and (v['SourceEngine']=='document_disclosure_review' or bank_scope_default)
                      and not scope_conflict(code,'standalone',v['RawValue'],v['SourceText'])]
            if (period,code) in manual:pool=[manual[(period,code)]]
            pool.sort(key=lambda v:(v['AccountingBasisCode']=='unknown',v['AccountingBasisCode']=='IFRS',v['Method']!='native_text','审计' in v['Title'],v['ObservationId']))
            if not pool: continue
            chosen=pool[0]; selected[code]=chosen
            comparable=[v for v in pool if all(v[k]==chosen[k] for k in ('EntityScopeCode','AccountingBasisCode','CurrencyCode','MeasurementBasisCode','PeriodStart','PeriodBasis'))]
            numeric=sorted({str(v['NumericValue']) for v in comparable})
            notes[code]={'candidate_count':len(pool),'comparable_values':numeric,'conflict':len(numeric)>1,
                         'issues':(['accounting_basis_not_stated'] if chosen['AccountingBasisCode']=='unknown' else [])+
                         (['entity_scope_not_confirmed'] if chosen['EntityScopeCode']=='unknown' else [])+
                         (['ocr_requires_visual_review'] if chosen['Method']=='paddleocr' else []),
                         'selected_attempt':chosen['AttemptId'],'source_page':chosen['PdfPage'],'run_id':CONFIG['run_id']}
            if chosen.get('NormalizationVersion')=='scope-default-standalone-v1':
                notes[code].update(scope_defaulted=True,scope_policy='scope-default-standalone-20261020')
            if (period,code) in manual:notes[code]=json.loads(manual[(period,code)]['ReviewNote'] or '{}')
            if (period,code) in baseline:
                previous=baseline[(period,code)]
                assert str(chosen['ObservationId'])==str(previous['ObservationId'])
                notes[code]=dict(json.loads(previous.get('ReviewNote') or '{}'),carried_from_record=previous['FinancialRecordId'],extension_run_id=CONFIG['run_id'])
        bases={selected[k]['AccountingBasisCode'] for k in METRICS if k not in METRICS[1:4] and k in selected}
        basis=next(iter(bases)) if len(bases)==1 else 'unknown'
        # Selection content participates in idempotency: adding validated evidence creates a new version.
        missing={}
        for code in METRICS:
            if code in selected:continue
            if any(v['MetricCode']==code and partial_profit_scope(code,v['RawValue'],v['SourceText']) for v in values):
                missing[code]={'code':'scope_pending','message':'仅披露持续经营税前利润；含终止经营业务的集团利润总额依据不足，待核对。'}
                gap_audit.append(dict(period=period,metric=code,**missing[code]))
                continue
            if (period,code) in baseline:
                previous=baseline[(period,code)]
                assert previous['ObservationId'] is None
                note=json.loads(previous.get('ReviewNote') or '{}')
                missing[code]=note.get('missing_evidence') or {'code':'prior_review_pending','message':previous.get('MissingReason') or '沿用历史待复核缺项。'}
                gap_audit.append(dict(period=period,metric=code,**missing[code]))
                continue
            reports=[d for d in docs if d['period_end']==period]
            for doc in reports:
                if doc['sha256'] not in page_sets:page_sets[doc['sha256']]=merged_pages(doc)
            hashes={d['sha256'] for d in reports}
            errors=[e for a in bundle['attempts'] if a['sha256'] in hashes for e in a['validation_errors']]
            missing[code]=classify_gap(code,reports,page_sets,errors)
            gap_audit.append(dict(period=period,metric=code,**missing[code]))
        policy=dict(POLICY,selected={k:v['ObservationId'] for k,v in selected.items()},period=period)
        presentation='mixed_pending' if any(v['EntityScopeCode'] in ('unknown','standalone') for v in selected.values()) else 'consolidated'
        if presentation=='mixed_pending':policy['disclosed_scope_review']='value_verified_scope_unconfirmed_v1'
        if baseline:policy['baseline_sha256']=CONFIG['baseline_sha256']
        # Classification is versioned with the draft; earlier records remain unchanged.
        if missing:policy['missing_evidence']=missing
        digest=sha(encode(policy))
        series=cn.execute("SELECT SeriesId FROM DV.IssuerFinancialSeries WITH(UPDLOCK,HOLDLOCK) WHERE IssuerId=? AND PeriodStart=? AND PeriodEnd=? AND FlowBasis='year_to_date' AND PresentationScopeCode=? AND AccountingBasisCode=? AND CurrencyCode='CNY' AND SelectionProfileCode='group_pilot'",issuer,period[:4]+'-01-01',period,presentation,basis).fetchval()
        if not series:
            series=cn.execute("INSERT DV.IssuerFinancialSeries(IssuerId,PeriodStart,PeriodEnd,FlowBasis,PresentationScopeCode,AccountingBasisCode,CurrencyCode,SelectionProfileCode) OUTPUT inserted.SeriesId VALUES(?,?,?,'year_to_date',?,?,'CNY','group_pilot')",issuer,period[:4]+'-01-01',period,presentation,basis).fetchval()
        record=cn.execute('SELECT FinancialRecordId FROM DV.IssuerFinancialRecord WHERE SeriesId=? AND SelectionPolicyHash=?',series,digest).fetchval()
        if not record:
            version=cn.execute('UPDATE DV.IssuerFinancialSeries SET NextVersionNo=NextVersionNo+1 OUTPUT deleted.NextVersionNo WHERE SeriesId=?',series).fetchval()
            primary=selected.get('total_assets',next(iter(selected.values()),{})).get('DocumentId',next(d['document_id'] for d in docs if d['period_end']==period))
            record=cn.execute("INSERT DV.IssuerFinancialRecord(SeriesId,IssuerId,VersionNo,SelectionPolicyVersion,SelectionPolicyHash,PrimaryDocumentId) OUTPUT inserted.FinancialRecordId VALUES(?,?,?,?,?,?)",series,issuer,version,POLICY['version'],digest,primary).fetchval()
            for code in METRICS:
                chosen=selected.get(code)
                note=notes.get(code,{'issues':[missing[code]['code']],'missing_evidence':missing[code],'run_id':CONFIG['run_id']} if code in missing else {})
                reviewed=manual.get((period,code),{})
                cn.execute("INSERT DV.IssuerFinancialMetric(FinancialRecordId,IssuerId,MetricCode,ObservationId,ValueStatus,MissingReason,ReviewStatus,ReviewNote,ReviewedBy,ReviewedAt) VALUES(?,?,?,?,?,?,?,?,?,?)",record,issuer,code,chosen['ObservationId'] if chosen else None,'reported' if chosen else 'unresolved',missing[code]['message'] if code in missing else None,reviewed.get('ReviewStatus','pending'),json.dumps(note,ensure_ascii=False),reviewed.get('ReviewedBy'),reviewed.get('ReviewedAt'))
            cn.execute("INSERT DV.IssuerChangeLog(EntityType,EntityId,Action,AfterJson,Reason,ActorId,RequestId,RequestHash) VALUES('record',?,'create_draft',?,N'按主体和运行版本选择；历史草稿保留','codex-pilot',?,?)",record,encode(policy).decode(),'issuer-v2-draft-'+str(record),digest)
            created.append(record)
        record_ids.append(record); cn.commit()
    result=export(cn,issuer_id=issuer,root_task_id=root,record_ids=record_ids)
    result['issuer_name']=ISSUER; result['display_name']=CONFIG['display_name']; result['run_id']=CONFIG['run_id']; result['policy']=POLICY
    result['all_database_counts']=result['counts']; result['records']=[r for r in result['records'] if int(r['FinancialRecordId']) in record_ids]
    result['metrics']=[m for m in result['metrics'] if int(m['FinancialRecordId']) in record_ids]
    obs={m['ObservationId'] for m in result['metrics'] if m['ObservationId']}
    result['evidence']=[e for e in result['evidence'] if e['ObservationId'] in obs]
    doc_ids={str(d['document_id']) for d in docs}; result['files']=[f for f in result['files'] if f['DocumentId'] in doc_ids]
    attempts={str(r[0]) for r in cn.execute('SELECT a.AttemptId FROM DV.IssuerProcessingAttempt a JOIN DV.IssuerProcessingTask t ON t.TaskId=a.TaskId WHERE t.RootTaskId=?',root)}
    result['attempts']=[a for a in result['attempts'] if a['AttemptId'] in attempts]
    tasks={str(r[0]) for r in cn.execute('SELECT TaskId FROM DV.IssuerProcessingTask WHERE RootTaskId=? OR TaskId=?',root,root)}
    result['tasks']=[t for t in result['tasks'] if t['TaskId'] in tasks]
    if baseline:
        current_periods={r['FinancialRecordId']:str(r['PeriodEnd'])[:10] for r in result['records']}
        for m in result['metrics']:
            old=baseline.get((current_periods[m['FinancialRecordId']],m['MetricCode']))
            if old:
                assert all(str(m.get(k))==str(old.get(k)) for k in ('ObservationId','NumericValue','ValueStatus','EntityScopeCode','AccountingBasisCode','UnitCode'))
    result['counts']={'Issuer':1,'IssuerFile':len(result['files']),'IssuerDocument':len(result['files']),
                      'IssuerMetricObservation':cn.execute('SELECT COUNT(*) FROM DV.IssuerMetricObservation o JOIN DV.IssuerProcessingAttempt a ON a.AttemptId=o.AttemptId JOIN DV.IssuerProcessingTask t ON t.TaskId=a.TaskId WHERE t.RootTaskId=?',root).fetchval(),
                      'IssuerFinancialRecord':len(record_ids),'IssuerFinancialMetric':len(result['metrics'])}
    check={'issuer_id':issuer,'root_task_id':root,'files':len({d['sha256'] for d in docs}),'source_paths':len(docs),'periods':len(record_ids),'selected':sum(m['ValueStatus']=='reported' for m in result['metrics']),
           'unresolved':sum(m['ValueStatus']!='reported' for m in result['metrics']),
           'unknown_amount_basis':sum(m['MetricCode'] not in METRICS[1:4] and m['AccountingBasisCode']=='unknown' for m in result['metrics']),
           'unknown_entity_scope':sum(m['ObservationId'] is not None and m['EntityScopeCode']=='unknown' for m in result['metrics']),
           'ratio_basis_errors':sum(bool(m['MetricCode'] in METRICS[1:4] and m['ObservationId'] and m['AccountingBasisCode']!='not_applicable') for m in result['metrics']),
           'cross_issuer_evidence':cn.execute('SELECT COUNT(*) FROM DV.IssuerObservationEvidence e JOIN DV.IssuerMetricObservation o ON o.ObservationId=e.ObservationId JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId WHERE e.IssuerId<>o.IssuerId OR e.IssuerId<>d.IssuerId').fetchval(),
           'unfinished_children':cn.execute("SELECT COUNT(*) FROM DV.IssuerProcessingTask WHERE RootTaskId=? AND Status NOT IN ('succeeded','cancelled')",root).fetchval(),
           'published':result['published_count'],'created_records':created}
    assert check['cross_issuer_evidence']==0 and check['ratio_basis_errors']==0 and check['published']==0
    outcome=('business_review_pending' if PRODUCTION else 'business_review_and_archive_pending') if not check['unfinished_children'] else 'extraction_incomplete'
    cn.execute("UPDATE DV.IssuerProcessingTask SET Status='needs_review',OutcomeCode=?,FinishedAt=SYSUTCDATETIME() WHERE TaskId=?",outcome,root)
    cn.commit();cn.close()
    atomic_json(ARTIFACTS/'sql-readback.json',result); atomic_json(ARTIFACTS/'verification.json',check)
    atomic_json(ARTIFACTS/'missing-evidence-review.json',gap_audit)
    print(json.dumps(check,ensure_ascii=False),flush=True)


if __name__=='__main__': main()
