"""Reconcile full-batch results against the frozen scope and current SQL pointers."""
import argparse
from collections import Counter
import copy
from decimal import Decimal
import json
from pathlib import Path
from ai_review import MODELS,query,save
from ai_review_batch import assess,context
from pilot_runtime import guarded_connection


def main(root):
    snapshot=json.loads((root/'snapshot.json').read_text());records={r['FinancialRecordId']:r for r in snapshot['records']}
    results=[]
    for p in (root/'results').glob('*.json'):results.extend(json.loads(p.read_text())['results'])
    expected={(r['FinancialRecordId'],m['MetricCode']) for r in records.values() for m in r['metrics']}
    actual=[(r['record_id'],r['metric']) for r in results]
    assert len(actual)==len(set(actual)), 'Duplicate result keys'
    assert set(actual)<=expected, 'Unexpected result keys'
    missing=sorted(expected-set(actual));counts=Counter(r['status'] for r in results)
    controls=0
    for record in records.values():
        checked=[r for r in results if r['record_id']==record['FinancialRecordId'] and r['status']=='checks_passed']
        if not checked:continue
        ctx=context(root,[record],expanded=True);pages={(p['document_id'],p['page']):p for p in ctx['pages']}
        metrics={m['MetricCode']:m for m in record['metrics']}
        for result in checked:
            stored=metrics[result['metric']]
            for model in MODELS:
                answer=result['findings'][model]
                assert not assess(answer,stored,pages,record['InstitutionType'])
                for field,value in [('NumericValue',str(Decimal(stored['NumericValue'])+1)),('PeriodEnd','2000-01-01'),
                                    ('EntityScopeCode','standalone' if stored['EntityScopeCode']!='standalone' else 'accounting_consolidated')]:
                    bad=copy.deepcopy(stored);bad[field]=value
                    assert assess(answer,bad,pages,record['InstitutionType']), 'Negative control passed'
                    controls+=1
    cn=guarded_connection()
    try:
        live=query(cn,"""WITH ranked AS (SELECT r.FinancialRecordId,r.IssuerId,s.PeriodEnd,
          ROW_NUMBER() OVER(PARTITION BY r.IssuerId,s.PeriodEnd ORDER BY r.FinancialRecordId DESC) n
          FROM DV.IssuerFinancialRecord r JOIN DV.IssuerFinancialSeries s ON s.SeriesId=r.SeriesId
          JOIN DV.Issuer i ON i.IssuerId=r.IssuerId WHERE r.RecordStatus<>'withdrawn' AND i.MonitoringStatus='active')
          SELECT FinancialRecordId FROM ranked WHERE n=1""")
        current={r['FinancialRecordId'] for r in live};stale=sorted(set(records)-current);added=sorted(current-set(records))
        published=cn.execute('SELECT COUNT(*) FROM DV.vw_IssuerFinancialPublished').fetchval()
        metrics=query(cn,'SELECT FinancialRecordId,MetricCode,ObservationId FROM DV.IssuerFinancialMetric')
        live_obs={(m['FinancialRecordId'],m['MetricCode']):m['ObservationId'] for m in metrics}
        changed=[k for r in records.values() for m in r['metrics'] if live_obs.get(k:=(r['FinancialRecordId'],m['MetricCode']))!=m['ObservationId']]
    finally:cn.close()
    report={'expected_records':len(records),'expected_metrics':len(expected),'reviewed_records':len({r['record_id'] for r in results}),
            'counts':dict(counts),'missing':missing,'stale_records':stale,'new_records':added,'changed_observations':changed,
            'negative_controls_rejected':controls,'published_view_rows':published,'business_writes':False,
            'review_complete':not(missing or stale or added or changed or counts['request_failed'])}
    save(root/'verification.json',report);print(json.dumps(report))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);a=p.parse_args();main(a.root)
