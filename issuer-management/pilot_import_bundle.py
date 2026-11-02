"""Import validated candidates idempotently into the isolated SQL pilot."""
from decimal import Decimal
import gzip
import json
from pathlib import Path
import re

from pilot_runtime import ROOT,ARTIFACTS,CONFIG,PRODUCTION,atomic_json,guarded_connection


def main():
    data=json.loads(gzip.decompress((ROOT/'input/normalized-bundle.json.gz').read_bytes()))
    docs={d['sha256']:d for d in json.loads((ARTIFACTS/'registered.json').read_text(encoding='utf-8'))}
    cn=guarded_connection()
    bank_scope_default=PRODUCTION and cn.execute('SELECT InstitutionType FROM DV.Issuer WHERE IssuerId=?',next(iter(docs.values()))['issuer_id']).fetchval()=='bank'
    for batch in re.split(r'(?m)^GO\s*$',(Path(__file__).parent/'pilot_migration_003_evidence.sql').read_text(encoding='utf-8')):
        if batch.strip():cn.execute(batch)
    cn.commit();counts=[]
    for item in data['attempts']:
        doc=docs[item['sha256']]
        binding=cn.execute('SELECT t.IssuerId,t.DocumentId,a.TaskId,a.InputHash,a.LeaseEpoch FROM DV.IssuerProcessingAttempt a JOIN DV.IssuerProcessingTask t ON t.TaskId=a.TaskId WHERE a.AttemptId=?',item['attempt_id']).fetchone()
        if not binding or tuple(binding)!=(doc['issuer_id'],doc['document_id'],item['task_id'],bytes.fromhex(item['input_hash']),item['lease_epoch']):
            raise RuntimeError('Attempt/document/issuer/input binding mismatch')
        task=cn.execute('SELECT LeaseEpoch,Status,LeaseExpiresAt FROM DV.IssuerProcessingTask WITH(UPDLOCK,HOLDLOCK) WHERE TaskId=?',item['task_id']).fetchone()
        # A successfully imported attempt can be replayed without reusing an expired lease.
        existing=cn.execute('SELECT Status FROM DV.IssuerProcessingAttempt WHERE AttemptId=?',item['attempt_id']).fetchval()
        if existing!='succeeded':
            active=cn.execute("SELECT 1 FROM DV.IssuerProcessingTask WHERE TaskId=? AND LeaseEpoch=? AND Status='running' AND LeaseExpiresAt>SYSUTCDATETIME()",item['task_id'],item['lease_epoch']).fetchval()
            if not active:raise RuntimeError('Expired or superseded task lease rejected')
        # The task row is locked above: fetch existing ordinals once per attempt,
        # avoiding one SQL round trip per candidate on every replay.
        imported_ordinals={r[0] for r in cn.execute('SELECT CandidateOrdinal FROM DV.IssuerMetricObservation WHERE AttemptId=?',item['attempt_id'])}
        inserted=0
        for c in item['candidates']:
            if c['ordinal'] in imported_ordinals:continue
            scope=c['entity_scope'];label=c['scope_label'];normalization='evidence-v3' if CONFIG else 'decimal-v2'
            if bank_scope_default and scope=='unknown':
                scope='standalone';label=(label+'；2026-10-20业务规则：未明确集团/合并，按本行处理。')[:512];normalization='scope-default-standalone-v1'
            obs=cn.execute("INSERT DV.IssuerMetricObservation(IssuerId,MetricCode,PeriodStart,PeriodEnd,PeriodBasis,EntityScopeCode,AccountingBasisCode,CurrencyCode,MeasurementBasisCode,MeasurementBasisLabel,NumericValue,UnitCode,OriginalLabel,OriginalScopeLabel,NormalizationVersion,AttemptId,CandidateOrdinal,Origin,CreatedBy) OUTPUT inserted.ObservationId VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'machine',?)",doc['issuer_id'],c['metric_code'],c['period_start'],c['period_end'],c['period_basis'],scope,c['accounting_basis'],c['currency'],c['measurement_code'],c['measurement_label'],Decimal(c['numeric_value']),c['unit_code'],c['original_label'],label,normalization,item['attempt_id'],c['ordinal'],item.get('created_by','goldencode')).fetchval()
            for e in c['evidence']:
                page=data['pages'][e['page_key']]
                cn.execute("INSERT DV.IssuerObservationEvidence(IssuerId,ObservationId,DocumentId,PdfPage,RegionJson,RawText,RawValue,RawUnit,ColumnHeader,Method,EvidenceRole) VALUES(?,?,?,?,?,?,?,?,?,?,?)",doc['issuer_id'],obs,doc['document_id'],page['page'],json.dumps(page['region'],ensure_ascii=False) if page['region'] else None,page['text'],c['raw_value'] if e['role']=='value' else '',c['raw_unit'] if e['role']=='value' else '',c['column_header'],page['method'],e['role'])
            imported_ordinals.add(c['ordinal']);inserted+=1
        usage=item['usage']
        cn.execute("UPDATE DV.IssuerProcessingAttempt SET Status='succeeded',ResultStorageKey=COALESCE(ResultStorageKey,?),ResultHash=?,GatewayRequestId=?,InputTokens=?,OutputTokens=?,UsageStatus=?,FinishedAt=COALESCE(FinishedAt,SYSUTCDATETIME()) WHERE AttemptId=? AND Status<>'succeeded'",item['result_storage_key'],bytes.fromhex(item['result_hash']),item['request_id'],usage.get('prompt_tokens'),usage.get('completion_tokens'),'known' if usage else 'unknown',item['attempt_id'])
        cn.execute("UPDATE DV.IssuerProcessingTask SET Status='succeeded',OutcomeCode=?,FinishedAt=SYSUTCDATETIME(),LeaseOwner=NULL,LeaseExpiresAt=NULL WHERE TaskId=? AND LeaseEpoch=? AND Status<>'succeeded'",'candidates_partial' if item['validation_errors'] else 'candidates_ready',item['task_id'],item['lease_epoch'])
        cn.commit();counts.append({'attempt_id':item['attempt_id'],'inserted':inserted,'validated':len(item['candidates']),'validation_errors':item['validation_errors']})
    cn.close();atomic_json(ARTIFACTS/'candidate-import.json',counts)
    print(json.dumps({'event':'candidates_imported','inserted':sum(c['inserted'] for c in counts),'attempts':counts},ensure_ascii=False),flush=True)


if __name__=='__main__':main()
