"""Validate an immutable baseline when extending six-metric drafts with two profits."""
import hashlib
import json

PROFIT_CODES = ['net_profit','net_profit_attributable_to_parent']
BASE_CODES={'total_assets','capital_adequacy_ratio','nonperforming_loan_ratio','provision_coverage_ratio','operating_revenue','total_profit'}


def load_baseline(root,config,issuer):
    if not config.get('extract_only'):return {},[]
    if config['extract_only']!=PROFIT_CODES:raise ValueError('Unsupported extension metrics')
    raw=(root/'input/baseline.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=config.get('baseline_sha256'):raise ValueError('Baseline digest mismatch')
    data=json.loads(raw)
    if data['issuer_name']!=issuer:raise ValueError('Baseline issuer mismatch')
    periods={r['FinancialRecordId']:str(r['PeriodEnd'])[:10] for r in data['records']}
    if len(set(periods.values()))!=len(periods):raise ValueError('Baseline must select one record per period')
    result={}
    for metric in data['metrics']:
        key=(periods[metric['FinancialRecordId']],metric['MetricCode'])
        if key in result or metric['MetricCode'] not in BASE_CODES:raise ValueError('Ambiguous extension baseline')
        if metric['ValueStatus'] not in ('reported','unresolved') or metric['ReviewStatus']!='pending':raise ValueError('Reviewed baseline needs a separate extension decision')
        result[key]=metric
    if len(result)!=6*len(periods):raise ValueError('Baseline must contain six metrics per period')
    return result, sorted({int(m['ObservationId']) for m in result.values() if m['ObservationId']})
