"""Small append-only review service; no publishing or original-row updates."""
import hashlib
import json
import re
from datetime import date
from decimal import Decimal, InvalidOperation

CODES=('total_assets','capital_adequacy_ratio','nonperforming_loan_ratio','provision_coverage_ratio',
       'operating_revenue','total_profit','net_profit','net_profit_attributable_to_parent')
SCOPES=('accounting_consolidated','regulatory_consolidated','standalone')
POLICY='scope-default-standalone-20261020'

class ReviewError(Exception):
    def __init__(self,message,status='400 Bad Request'):
        super().__init__(message);self.status=status

def encode(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,default=str)

def rows(cn,sql,*args):
    c=cn.execute(sql,*args);keys=[x[0] for x in c.description]
    return [dict(zip(keys,r)) for r in c.fetchall()]

def checked(payload):
    p=dict(payload)
    for key in ('issuer_id','expected_record_id','document_id','page'):
        if not re.fullmatch(r'[1-9]\d{0,17}',str(p.get(key,''))):raise ReviewError('主体、报告或页码无效。')
        p[key]=int(p[key])
    if p.get('metric_code') not in CODES:raise ReviewError('指标无效。')
    if not re.fullmatch(r'[A-Za-z0-9_-]{16,100}',str(p.get('request_id',''))):raise ReviewError('请求标识无效。')
    try:date.fromisoformat(p['period_end'])
    except (KeyError,TypeError,ValueError):raise ReviewError('报告期无效。')
    for key,limit in (('reviewer',80),('reason',2000),('quote',20000)):
        p[key]=str(p.get(key,'')).strip()
        if not p[key] or len(p[key])>limit:raise ReviewError('请填写核对人、核对说明及原文摘录。')
    if p.get('confirmed') is not True:raise ReviewError('请先确认数值、当期列和原文。')
    raw=str(p.get('raw_value','')).replace(',','').strip()
    if not re.fullmatch(r'-?\d{1,22}(?:\.\d{1,6})?',raw):raise ReviewError('请填写普通十进制数字，不使用科学计数。')
    multipliers={'元':Decimal(1),'千元':Decimal(1000),'万元':Decimal(10000),'百万元':Decimal(1000000),'亿元':Decimal(100000000),'%':Decimal(1)}
    unit=p.get('raw_unit');ratio=p['metric_code'] in CODES[1:4]
    if unit not in multipliers or (unit=='%')!=ratio:raise ReviewError('单位与指标不一致。')
    value=Decimal(raw)*multipliers[unit]
    if not value.is_finite() or abs(value)>=Decimal('1e22'):raise ReviewError('数值超过支持范围。')
    if ratio and (value<0 or (p['metric_code']!='provision_coverage_ratio' and value>100)):raise ReviewError('百分比范围无效。')
    scope=p.get('scope','unknown');p['scope_defaulted']=scope=='unknown'
    if scope=='unknown':scope='standalone'
    if scope not in SCOPES:raise ReviewError('口径无效。')
    if p['metric_code']=='capital_adequacy_ratio' and scope=='accounting_consolidated':raise ReviewError('资本充足率请选监管并表或本行。')
    if not ratio and scope=='regulatory_consolidated':raise ReviewError('财务金额请选会计集团或本行。')
    basis=p.get('basis','unknown')
    if ratio:basis='not_applicable'
    elif basis not in ('CAS','IFRS','unknown'):raise ReviewError('会计准则无效。')
    p.update(scope=scope,basis=basis,numeric=value,unit='percent' if ratio else 'yuan',
             currency='not_applicable' if ratio else 'CNY',
             measurement='not_specified' if ratio else 'not_applicable',
             period_start=p['period_end'] if p['metric_code'] in CODES[:4] else p['period_end'][:4]+'-01-01',
             period_basis='instant' if p['metric_code'] in CODES[:4] else 'year_to_date')
    return p

def validate_eligibility(issuer,p):
    if not issuer or issuer['MonitoringStatus']!='active':
        raise ReviewError('该主体暂停采用，不能补录。','409 Conflict')
    kind=issuer['InstitutionType']
    if kind not in ('bank','amc'):raise ReviewError('该机构类型暂不开放核对录入。')
    if kind=='amc':
        if p['metric_code'] in CODES[1:4]:
            raise ReviewError('资产管理公司不在此处录入银行监管指标；定义不同的指标不混用。')
        if p.get('definition_confirmed') is not True:
            raise ReviewError('请确认原文指标定义与所选财务指标一致；定义不同不录入。')
        if p['scope_defaulted']:
            raise ReviewError('资产管理公司的财务指标需根据原文明确选择集团／合并或母公司口径。')

def apply_review(cn,payload,*,internal=False):
    """Caller owns transaction. Lock, validate, append observation/evidence/version/log."""
    p=checked(payload);digest=hashlib.sha256(encode(payload).encode()).digest()
    lock=cn.execute("""DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=?,@LockMode='Exclusive',
        @LockOwner='Transaction',@LockTimeout=10000; SELECT @r""",'issuer-review-'+str(p['issuer_id'])).fetchval()
    if lock<0:raise ReviewError('另一项核对正在保存，请重试。','409 Conflict')
    prior=rows(cn,"SELECT RequestHash,AfterJson FROM DV.IssuerChangeLog WHERE EntityType='issuer' AND EntityId=? AND Action='confirm_metric' AND RequestId=?",p['issuer_id'],p['request_id'])
    if prior:
        if bytes(prior[0]['RequestHash'])!=digest:raise ReviewError('重复请求的内容不一致。','409 Conflict')
        return dict(json.loads(prior[0]['AfterJson'])['result'],replayed=True)
    issuer=rows(cn,'SELECT MonitoringStatus,InstitutionType FROM DV.Issuer WHERE IssuerId=?',p['issuer_id'])
    validate_eligibility(issuer[0] if issuer else None,p)
    records=rows(cn,"""SELECT TOP 1 r.* FROM DV.IssuerFinancialRecord r
       JOIN DV.IssuerFinancialSeries s ON s.SeriesId=r.SeriesId
       WHERE r.IssuerId=? AND s.PeriodEnd=? AND r.RecordStatus<>'withdrawn' ORDER BY r.FinancialRecordId DESC""",p['issuer_id'],p['period_end'])
    if not records or records[0]['FinancialRecordId']!=p['expected_record_id']:raise ReviewError('数据已被更新，请刷新后重新核对。','409 Conflict')
    record=records[0]
    metrics=rows(cn,'SELECT * FROM DV.IssuerFinancialMetric WHERE FinancialRecordId=?',record['FinancialRecordId'])
    old=next((x for x in metrics if x['MetricCode']==p['metric_code']),None)
    if not old:raise ReviewError('该报告期未登记此指标。')
    if str(old['ObservationId'] or '')!=str(p.get('expected_observation_id') or ''):raise ReviewError('原值已改变，请刷新。','409 Conflict')
    docs=rows(cn,"""SELECT d.DocumentId,d.MainPeriodEnd,f.PageCount,CONVERT(varchar(64),f.Sha256,2) Sha256
      FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId
      WHERE d.DocumentId=? AND d.IssuerId=? AND d.DocumentStatus='active' AND f.ArchiveStatus='archived'""",p['document_id'],p['issuer_id'])
    if not docs or not 1<=p['page']<=docs[0]['PageCount']:raise ReviewError('证据文件或页码无效。')
    if str(docs[0]['MainPeriodEnd'])[:10]!=p['period_end']:raise ReviewError('请选择该报告期对应的原文。')
    if docs[0]['Sha256'].lower()!=str(p.get('document_sha256','')).lower():raise ReviewError('原文版本已改变，请刷新。','409 Conflict')
    evidence_source=payload.get('copy_evidence_observation') if internal else None
    if evidence_source:
        source=rows(cn,'SELECT * FROM DV.IssuerMetricObservation WHERE ObservationId=? AND IssuerId=? AND MetricCode=?',
                    int(evidence_source),p['issuer_id'],p['metric_code'])
        if not source or source[0]['NumericValue']!=p['numeric'] or str(source[0]['PeriodEnd'])!=p['period_end']:
            raise ReviewError('原观察值的指标、数值或报告期不匹配。')
        source=source[0]
        p.update(measurement=source['MeasurementBasisCode'],period_start=str(source['PeriodStart']),
                 period_basis=source['PeriodBasis'])
    scope_note=('按业务规则归为本行；原披露未明确集团或合并口径。' if p['scope_defaulted'] else '核对人确认披露范围：'+p['scope'])
    obs=cn.execute("""INSERT DV.IssuerMetricObservation(IssuerId,MetricCode,PeriodStart,PeriodEnd,PeriodBasis,
      EntityScopeCode,AccountingBasisCode,CurrencyCode,MeasurementBasisCode,MeasurementBasisLabel,NumericValue,UnitCode,
      OriginalLabel,OriginalScopeLabel,NormalizationVersion,Origin,CreatedBy,CorrectionOfObservationId)
      OUTPUT inserted.ObservationId VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'reviewer',?,?)""",
      p['issuer_id'],p['metric_code'],p['period_start'],p['period_end'],p['period_basis'],p['scope'],p['basis'],
      p['currency'],p['measurement'],'人工核对',p['numeric'],p['unit'],p['metric_code'],scope_note,
      'manual-review-v1',p['reviewer'],old['ObservationId']).fetchval()
    if evidence_source:
        copied=cn.execute("""INSERT DV.IssuerObservationEvidence(IssuerId,ObservationId,DocumentId,PdfPage,RegionJson,
          RawText,RawValue,RawUnit,ColumnHeader,Method,EvidenceRole)
          SELECT IssuerId,?,DocumentId,PdfPage,RegionJson,RawText,RawValue,RawUnit,ColumnHeader,Method,EvidenceRole
          FROM DV.IssuerObservationEvidence WHERE IssuerId=? AND ObservationId=?""",obs,p['issuer_id'],int(evidence_source)).rowcount
        if copied<1:raise ReviewError('原观察值缺少可复制证据。')
    else:
        cn.execute("""INSERT DV.IssuerObservationEvidence(IssuerId,ObservationId,DocumentId,PdfPage,RawText,RawValue,RawUnit,
          ColumnHeader,Method,EvidenceRole) VALUES(?,?,?,?,?,?,?,?,'manual_review','value')""",
          p['issuer_id'],obs,p['document_id'],p['page'],p['quote'],p['raw_value'],p['raw_unit'],p['period_end'])
    source_series=rows(cn,'SELECT * FROM DV.IssuerFinancialSeries WHERE SeriesId=?',record['SeriesId'])[0]
    remaining=rows(cn,"""SELECT o.MetricCode,o.EntityScopeCode,o.AccountingBasisCode
      FROM DV.IssuerFinancialMetric m JOIN DV.IssuerMetricObservation o ON o.ObservationId=m.ObservationId
      WHERE m.FinancialRecordId=? AND m.MetricCode<>?""",record['FinancialRecordId'],p['metric_code'])
    combined=remaining+[dict(MetricCode=p['metric_code'],EntityScopeCode=p['scope'],AccountingBasisCode=p['basis'])]
    presentation='mixed_pending' if any(x['EntityScopeCode'] in ('standalone','unknown') for x in combined) else 'consolidated'
    bases={x['AccountingBasisCode'] for x in combined if x['MetricCode'] not in CODES[1:4]}
    basis=next(iter(bases)) if len(bases)==1 else 'unknown'
    series=cn.execute("""SELECT SeriesId FROM DV.IssuerFinancialSeries WHERE IssuerId=? AND PeriodStart=? AND PeriodEnd=?
      AND FlowBasis=? AND PresentationScopeCode=? AND AccountingBasisCode=? AND CurrencyCode=? AND SelectionProfileCode=?""",
      p['issuer_id'],source_series['PeriodStart'],source_series['PeriodEnd'],source_series['FlowBasis'],presentation,basis,
      source_series['CurrencyCode'],source_series['SelectionProfileCode']).fetchval()
    if not series:
        series=cn.execute("""INSERT DV.IssuerFinancialSeries(IssuerId,PeriodStart,PeriodEnd,FlowBasis,PresentationScopeCode,
          AccountingBasisCode,CurrencyCode,SelectionProfileCode) OUTPUT inserted.SeriesId VALUES(?,?,?,?,?,?,?,?)""",
          p['issuer_id'],source_series['PeriodStart'],source_series['PeriodEnd'],source_series['FlowBasis'],presentation,basis,
          source_series['CurrencyCode'],source_series['SelectionProfileCode']).fetchval()
    version=cn.execute('UPDATE DV.IssuerFinancialSeries SET NextVersionNo=NextVersionNo+1 OUTPUT deleted.NextVersionNo WHERE SeriesId=?',series).fetchval()
    new_record=cn.execute("""INSERT DV.IssuerFinancialRecord(SeriesId,IssuerId,VersionNo,SelectionPolicyVersion,
        SelectionPolicyHash,PrimaryDocumentId,RestatesRecordId) OUTPUT inserted.FinancialRecordId VALUES(?,?,?,?,?,?,?)""",
        series,p['issuer_id'],version,'manual-review-v1',digest,p['document_id'],record['FinancialRecordId'] if series==record['SeriesId'] else None).fetchval()
    cn.execute("""INSERT DV.IssuerFinancialMetric(FinancialRecordId,IssuerId,MetricCode,ObservationId,ValueStatus,MissingReason,
       ReviewStatus,ReviewNote,ReviewedBy,ReviewedAt)
       SELECT ?,IssuerId,MetricCode,ObservationId,ValueStatus,MissingReason,ReviewStatus,ReviewNote,ReviewedBy,ReviewedAt
       FROM DV.IssuerFinancialMetric WHERE FinancialRecordId=? AND MetricCode<>?""",new_record,record['FinancialRecordId'],p['metric_code'])
    note={'manual_review':True,'reason':p['reason'],'scope_defaulted':p['scope_defaulted'],
          'scope_policy':POLICY if p['scope_defaulted'] else None,'previous_record_id':record['FinancialRecordId'],
          'previous_observation_id':old['ObservationId'],'source_observation_id':evidence_source,
          'business_adoption':'pending','conflict':False}
    review_status='pending' if internal else 'verified'
    cn.execute("""INSERT DV.IssuerFinancialMetric(FinancialRecordId,IssuerId,MetricCode,ObservationId,ValueStatus,
       ReviewStatus,ReviewNote,ReviewedBy,ReviewedAt) VALUES(?,?,?,?,'reported',?,?,?,SYSUTCDATETIME())""",
       new_record,p['issuer_id'],p['metric_code'],obs,review_status,encode(note),p['reviewer'])
    result={'record_id':str(new_record),'observation_id':str(obs),'review_status':review_status,'published':False}
    cn.execute("""INSERT DV.IssuerChangeLog(EntityType,EntityId,Action,BeforeJson,AfterJson,Reason,ActorId,RequestId,RequestHash)
       VALUES('issuer',?,'confirm_metric',?,?,?,?,?,?)""",p['issuer_id'],encode(old),encode({'result':result,'review':p,'note':note}),
       p['reason'],p['reviewer'],p['request_id'],digest)
    return result
