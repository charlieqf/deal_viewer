"""Pass pending values whose AI reading matches and whose trend agrees with verified neighbours.

TREND_RULE (business decision 2026-11-03). No model is called and nothing is written to
business tables. A pending reported value passes only when both hold:
1. a completed AI review run read exactly the stored value, in its metric and period row,
   from a quote found on the cited page;
2. verified values of the same issuer, metric and comparable basis (scope, standard with
   "not stated" read as CAS, method, unit) confirm its size:
   - ratios lie within 5% (relative) of the two nearest verified periods (one each side
     when both exist);
   - cumulative revenue and profit rise within the year, strictly between the verified
     earlier quarter (zero for the first quarter) and later quarter of that year. Without
     a later verified quarter (the latest period or the annual figure), and always for the
     first quarter, the value must also be within 30% of the verified figure a year earlier.
Values passed by this rule never serve as references, so passes cannot chain.
The output directory is accepted by ai_review_writeback.py like any review run.
"""
import argparse
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from ai_review import query, save
from ai_review_batch import stamp

VERSION = 'issuer-trend-review-v1'
RULE = 'trend-20261103'
RATIOS = ('capital_adequacy_ratio', 'nonperforming_loan_ratio', 'provision_coverage_ratio')
FLOWS = ('operating_revenue', 'total_profit', 'net_profit', 'net_profit_attributable_to_parent')
DONE = ('verified', 'corrected')
RATIO_BAND = Decimal('0.05')
YOY_BAND = Decimal('0.30')
NEAR_DAYS = 400
# Any of these means a model's answer did not read this exact value from this metric's row.
READ_ERRORS = {'model_uncertain', 'value_mismatch', 'invalid_value_or_unit', 'unit_kind_mismatch',
               'number_missing_in_quote', 'value_quote_unverifiable', 'metric_mismatch', 'period_end_mismatch'}


def basis(m):
    # Business rule: an amount whose report names no accounting standard follows CAS.
    return 'CAS' if m['UnitCode'] != 'percent' and m['AccountingBasisCode'] == 'unknown' else m['AccountingBasisCode']


def series(m):
    # "Not stated" and "not applicable" both name no measurement method.
    method = 'not_applicable' if m['MeasurementBasisCode'] == 'not_specified' else m['MeasurementBasisCode']
    return (m['IssuerId'], m['MetricCode'], m['EntityScopeCode'], basis(m), method, m['UnitCode'], m['PeriodBasis'])


def reference(m):
    # Write-back records the rule in the review note; values it passed are not references.
    return (m['ValueStatus'] == 'reported' and m['ReviewStatus'] in DONE and m['EntityScopeCode'] != 'unknown'
            and RULE not in (m['ReviewNote'] or ''))


def day(m):
    return date.fromisoformat(str(m['PeriodEnd'])[:10])


def cite(m):
    return {'period_end': str(day(m)), 'value': str(m['NumericValue']), 'record_id': m['FinancialRecordId'],
            'observation_id': m['ObservationId']}


def number(m):
    return Decimal(str(m['NumericValue']))


def near(value, base, band):
    # Relative difference without division; a non-positive base never matches.
    return abs(value - base) <= band * base


def judge(m, refs):
    """Return (rule, cited references) when verified neighbours confirm m, else (None, reason)."""
    value = number(m); when = day(m)
    if m['MetricCode'] in RATIOS:
        close = [r for r in refs if abs((day(r) - when).days) <= NEAR_DAYS]
        before = sorted((r for r in close if day(r) < when), key=day, reverse=True)
        after = sorted((r for r in close if day(r) > when), key=day)
        chosen = [before[0], after[0]] if before and after else (before or after)[:2]
        if len(chosen) < 2:return None, 'too_few_verified_neighbours'
        if not all(near(value, number(r), RATIO_BAND) for r in chosen):return None, 'outside_ratio_band'
        return 'ratio_band', [cite(r) for r in chosen]
    year = [r for r in refs if day(r).year == when.year]
    lower = max((r for r in year if day(r) < when), key=day, default=None)
    upper = min((r for r in year if day(r) > when), key=day, default=None)
    prior = next((r for r in refs if day(r) == date(when.year - 1, when.month, when.day)), None)
    if not (lower or (when.month, when.day) == (3, 31)):return None, 'no_verified_earlier_quarter'
    if not value > (number(lower) if lower else 0):return None, 'not_above_earlier_quarter'
    if upper and not value < number(upper):return None, 'not_below_later_quarter'
    if lower and upper:return 'within_year', [cite(lower), cite(upper)]
    # Zero is a weak lower bound and a missing later quarter no upper bound: compare a year earlier.
    if not prior:return None, 'no_verified_prior_year'
    if not near(value, number(prior), YOY_BAND):return None, 'outside_year_on_year_band'
    return 'year_on_year', [cite(r) for r in (lower, upper, prior) if r]


def readings(sources):
    """Models that read each (metric, observation) exactly, per completed review run."""
    reads = defaultdict(set); origin = {}
    for root in sources:
        verification = root/'verification.json'; state = json.loads(verification.read_text(encoding='utf-8'))
        if not state.get('review_complete') or state.get('business_writes'):raise ValueError('Source run is incomplete: '+root.name)
        origin[root.name] = hashlib.sha256(verification.read_bytes()).hexdigest()
        for path in (root/'results').glob('*.json'):
            for r in json.loads(path.read_text(encoding='utf-8'))['results']:
                # Rows without a reported value carry no 'findings'; a failed request has no answer.
                for model, answer in r.get('findings', {}).items():
                    if answer and not set(r['errors'][model]) & READ_ERRORS:
                        reads[(r['metric'], r['observation_id'])].add(root.name+':'+model)
    return reads, origin


def review(metrics, reads):
    """Judge every pending ratio/flow value; return results grouped by record."""
    pool = defaultdict(list)
    for m in metrics:
        if reference(m):pool[series(m)].append(m)
    results = defaultdict(list); counts = Counter()
    for m in metrics:
        if m['MetricCode'] not in RATIOS+FLOWS or m['ReviewStatus'] != 'pending' or m['ValueStatus'] != 'reported':continue
        out = {'record_id': m['FinancialRecordId'], 'issuer': m['LegalName'], 'metric': m['MetricCode'],
               'observation_id': m['ObservationId'], 'period_end': str(day(m)), 'value': str(m['NumericValue'])}
        read = sorted(reads.get((m['MetricCode'], m['ObservationId']), ()))
        rule, detail = judge(m, pool[series(m)]) if read else (None, 'no_exact_ai_reading')
        if rule:out.update(status='checks_passed', rule=rule, references=detail, exact_readings=read)
        else:out.update(status='trend_not_met', reason=detail)
        counts[out['status'] if rule else detail] += 1
        results[m['FinancialRecordId']].append(out)
    return results, counts


SELECTED = """WITH ranked AS (SELECT r.FinancialRecordId,r.IssuerId,s.PeriodEnd,
  ROW_NUMBER() OVER(PARTITION BY r.IssuerId,s.PeriodEnd ORDER BY r.FinancialRecordId DESC) n
  FROM DV.IssuerFinancialRecord r JOIN DV.IssuerFinancialSeries s ON s.SeriesId=r.SeriesId WHERE r.RecordStatus<>'withdrawn') """


def load(cn):
    metrics = query(cn, SELECTED+"""SELECT r.FinancialRecordId,r.IssuerId,r.PeriodEnd RecordPeriodEnd,i.LegalName,m.MetricCode,
      m.ValueStatus,m.ReviewStatus,m.ReviewNote,o.ObservationId,o.NumericValue,o.UnitCode,o.EntityScopeCode,
      o.AccountingBasisCode,o.MeasurementBasisCode,o.PeriodEnd,o.PeriodBasis
      FROM ranked r JOIN DV.Issuer i ON i.IssuerId=r.IssuerId JOIN DV.IssuerFinancialMetric m ON m.FinancialRecordId=r.FinancialRecordId
      LEFT JOIN DV.IssuerMetricObservation o ON o.ObservationId=m.ObservationId
      WHERE r.n=1 AND i.MonitoringStatus='active'""")
    evidence = query(cn, """SELECT e.ObservationId,e.DocumentId,e.PdfPage,CONVERT(varchar(64),f.Sha256,2) Sha256
      FROM DV.IssuerObservationEvidence e JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId AND d.IssuerId=e.IssuerId
      JOIN DV.IssuerFile f ON f.FileId=d.FileId WHERE d.DocumentStatus='active' AND f.ArchiveStatus='archived'""")
    return metrics, evidence


def run(metrics, evidence, sources, out):
    if (out/'verification.json').exists():raise ValueError('Trend review output already exists')
    reads, origin = readings(sources)
    results, counts = review(metrics, reads)
    by_record = defaultdict(list)
    for m in metrics:by_record[m['FinancialRecordId']].append(m)
    pages = defaultdict(lambda: defaultdict(set)); hashes = {}
    for e in evidence:
        pages[e['ObservationId']][e['DocumentId']].add(e['PdfPage']); hashes[e['DocumentId']] = e['Sha256']
    records = []
    for record_id, items in sorted(results.items()):
        passed = [r for r in items if r['status'] == 'checks_passed']
        save(out/'results'/f'{record_id}.json', {'version': VERSION, 'rule': RULE, 'results': items, 'business_writes': False})
        if not passed:continue
        docs = defaultdict(set)
        for r in passed:
            for doc, numbers in pages[r['observation_id']].items():docs[doc] |= numbers
        for doc in docs:save(out/'documents'/f'{doc}.json', {'DocumentId': doc, 'Sha256': hashes[doc]})
        rows = by_record[record_id]
        records.append({'FinancialRecordId': record_id, 'IssuerId': rows[0]['IssuerId'], 'LegalName': rows[0]['LegalName'],
                        'PeriodEnd': str(rows[0]['RecordPeriodEnd'])[:10],
                        'metrics': [{'MetricCode': m['MetricCode'], 'ObservationId': m['ObservationId']} for m in rows],
                        'evidence_pages': {str(k): sorted(v) for k, v in docs.items()}})
    save(out/'snapshot.json', {'version': VERSION, 'at': stamp(), 'records': records, 'groups': []})
    report = {'version': VERSION, 'rule': RULE, 'reason': 'AI读数与原文一致，且与已核对相邻期趋势吻合', 'readings_from': origin,
              'counts': dict(counts), 'model_calls': 0, 'business_writes': False, 'review_complete': True}
    save(out/'verification.json', report)
    return report


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--source', type=Path, action='append', required=True, help='Completed review run whose readings count (read only)')
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    from pilot_runtime import guarded_connection
    cn = guarded_connection()
    try:metrics, evidence = load(cn)
    finally:cn.close()
    print(json.dumps(run(metrics, evidence, a.source, a.out), ensure_ascii=False))


if __name__ == '__main__':main()
