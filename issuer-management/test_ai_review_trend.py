import json
import tempfile
import unittest
from pathlib import Path
import ai_review_trend as tr
import ai_review_writeback as wb

YI = 100000000


def metric(record, metric_code, period, value, status='verified', scope='accounting_consolidated', basis='CAS', note=None,
           observation=None, issuer=1):
    ratio = metric_code in tr.RATIOS
    return {'FinancialRecordId': record, 'IssuerId': issuer, 'RecordPeriodEnd': period, 'LegalName': '测试银行',
            'MetricCode': metric_code, 'ValueStatus': 'reported', 'ReviewStatus': status,
            'ReviewNote': json.dumps(note) if note else None, 'ObservationId': observation or record*10+len(metric_code),
            'NumericValue': str(value), 'UnitCode': 'percent' if ratio else 'yuan', 'EntityScopeCode': scope,
            'AccountingBasisCode': 'not_applicable' if ratio else basis,
            'MeasurementBasisCode': 'five_category' if ratio else 'not_applicable', 'PeriodEnd': period,
            'PeriodBasis': 'instant' if ratio else 'year_to_date'}


# 浦发 consolidated net profit (亿元), from the 2026-11-03 request.
PROFIT = {'2024-03-31': 176.60, '2024-06-30': 273.24, '2024-09-30': 356.87, '2024-12-31': 458.35,
          '2025-03-31': 177.76, '2025-06-30': 298.94, '2025-09-30': 391.71, '2025-12-31': 504.05}


def profits(pending, **changes):
    rows = []
    for n, (period, value) in enumerate(sorted(PROFIT.items()), 1):
        rows.append(metric(n, 'net_profit', period, int(changes.get(period, value)*YI),
                           status='pending' if period in pending else 'verified'))
    return rows


def judged(rows, reads=None):
    pending = [m for m in rows if m['ReviewStatus'] == 'pending']
    reads = {(m['MetricCode'], m['ObservationId']): {'run:goldencode'} for m in pending} if reads is None else reads
    results, _ = tr.review(rows, reads)
    return {(r['metric'], r['period_end']): r for items in results.values() for r in items}


class TrendTests(unittest.TestCase):
    def test_cumulative_profit_rises_between_verified_quarters(self):
        r = judged(profits({'2025-06-30'}))[('net_profit', '2025-06-30')]
        self.assertEqual((r['status'], r['rule']), ('checks_passed', 'within_year'))
        self.assertEqual([x['period_end'] for x in r['references']], ['2025-03-31', '2025-09-30'])
        # A misread value outside the verified neighbours stays for people.
        self.assertEqual(judged(profits({'2025-06-30'}, **{'2025-06-30': 29.894}))[('net_profit', '2025-06-30')]['reason'],
                         'not_above_earlier_quarter')
        self.assertEqual(judged(profits({'2025-06-30'}, **{'2025-06-30': 400.0}))[('net_profit', '2025-06-30')]['reason'],
                         'not_below_later_quarter')

    def test_year_end_and_first_quarter_also_need_year_on_year(self):
        r = judged(profits({'2025-12-31'}))[('net_profit', '2025-12-31')]
        self.assertEqual(r['rule'], 'year_on_year')  # 504.05 > Q3 391.71; +10% on 458.35.
        self.assertEqual(judged(profits({'2025-12-31'}, **{'2025-12-31': 700.0}))[('net_profit', '2025-12-31')]['reason'],
                         'outside_year_on_year_band')
        self.assertEqual(judged(profits({'2025-03-31'}))[('net_profit', '2025-03-31')]['rule'], 'year_on_year')
        # The first quarter's only lower bound is zero: without a verified prior year it stays pending.
        self.assertEqual(judged(profits({'2024-03-31'}))[('net_profit', '2024-03-31')]['reason'], 'no_verified_prior_year')

    def test_ratio_within_band_of_nearest_verified_periods(self):
        rows = [metric(1, 'nonperforming_loan_ratio', '2025-06-30', '1.38'), metric(2, 'nonperforming_loan_ratio', '2025-09-30', '1.36', 'pending'),
                metric(3, 'nonperforming_loan_ratio', '2025-12-31', '1.33'), metric(4, 'nonperforming_loan_ratio', '2026-03-31', '1.20')]
        r = judged(rows)[('nonperforming_loan_ratio', '2025-09-30')]
        self.assertEqual((r['rule'], [x['period_end'] for x in r['references']]), ('ratio_band', ['2025-06-30', '2025-12-31']))
        # Latest period: the two nearest earlier verified periods.
        rows = [metric(1, 'provision_coverage_ratio', '2026-03-31', '183.85'), metric(2, 'provision_coverage_ratio', '2026-06-30', '186.50'),
                metric(3, 'provision_coverage_ratio', '2026-09-30', '186.96', 'pending')]
        self.assertEqual(judged(rows)[('provision_coverage_ratio', '2026-09-30')]['rule'], 'ratio_band')
        # A core tier-one ratio taken for the capital adequacy ratio falls outside the band.
        rows = [metric(1, 'capital_adequacy_ratio', '2026-03-31', '13.40', scope='regulatory_consolidated'),
                metric(2, 'capital_adequacy_ratio', '2026-06-30', '13.55', scope='regulatory_consolidated'),
                metric(3, 'capital_adequacy_ratio', '2026-09-30', '9.52', 'pending', scope='regulatory_consolidated')]
        self.assertEqual(judged(rows)[('capital_adequacy_ratio', '2026-09-30')]['reason'], 'outside_ratio_band')

    def test_only_comparable_verified_references_count(self):
        rows = [metric(1, 'nonperforming_loan_ratio', '2025-06-30', '1.38', scope='standalone'),
                metric(2, 'nonperforming_loan_ratio', '2025-09-30', '1.36', 'pending'),
                metric(3, 'nonperforming_loan_ratio', '2025-12-31', '1.33', note={'ai_review': {'rule': tr.RULE}}),
                metric(4, 'nonperforming_loan_ratio', '2026-03-31', '1.35', 'pending')]
        self.assertEqual(judged(rows)[('nonperforming_loan_ratio', '2025-09-30')]['reason'], 'too_few_verified_neighbours')
        # "Not stated" amounts compare with CAS ones.
        rows = profits({'2025-06-30'})
        rows[4]['AccountingBasisCode'] = 'unknown'
        self.assertEqual(judged(rows)[('net_profit', '2025-06-30')]['rule'], 'within_year')
        rows[4]['EntityScopeCode'] = 'standalone'  # The 2025 Q1 lower bound no longer compares.
        self.assertEqual(judged(rows)[('net_profit', '2025-06-30')]['reason'], 'no_verified_earlier_quarter')

    def test_trend_alone_is_not_enough_without_an_exact_reading(self):
        rows = profits({'2025-06-30'})
        self.assertEqual(judged(rows, reads={})[('net_profit', '2025-06-30')]['reason'], 'no_exact_ai_reading')

    def test_output_feeds_writeback_and_readings_must_match(self):
        rows = profits({'2025-06-30', '2025-09-30'})
        target = next(m for m in rows if m['PeriodEnd'] == '2025-06-30')
        other = next(m for m in rows if m['PeriodEnd'] == '2025-09-30')
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder)/'full-v9'; (source/'results').mkdir(parents=True)
            (source/'verification.json').write_text(json.dumps({'review_complete': True, 'business_writes': False}))
            findings = {'goldencode': {'status': 'found'}, 'goldencode-d': {'status': 'found'}}
            results = [{'metric': 'net_profit', 'observation_id': target['ObservationId'], 'status': 'exception', 'findings': findings,
                        'errors': {'goldencode': ['scope_quote_unverifiable', 'basis_mismatch'], 'goldencode-d': ['value_mismatch']}},
                       # One model misread; the other's request failed, which leaves an error code but no answer.
                       {'metric': 'net_profit', 'observation_id': other['ObservationId'], 'status': 'request_failed',
                        'findings': {'goldencode': {'status': 'found'}, 'goldencode-d': None},
                        'errors': {'goldencode': ['value_mismatch'], 'goldencode-d': ['invalid_structured_response']}},
                       {'metric': 'total_assets', 'observation_id': 1, 'status': 'no_reported_value', 'errors': {'rules': ['unresolved']}}]
            (source/'results/1.json').write_text(json.dumps({'results': results}))
            evidence = [{'ObservationId': target['ObservationId'], 'DocumentId': 7, 'PdfPage': 3, 'Sha256': 'A'*64}]
            out = Path(folder)/'trend'
            report = tr.run(rows, evidence, [source], out)
            self.assertEqual(report['counts'], {'checks_passed': 1, 'no_exact_ai_reading': 1})
            verification, version, items = wb.candidates(out)
            self.assertEqual((version, verification['rule']), (tr.VERSION, tr.RULE))
            self.assertEqual([(i['record_id'], i['metric'], i['observation_id'], i['documents']) for i in items],
                             [(target['FinancialRecordId'], 'net_profit', target['ObservationId'], {7: 'a'*64})])
            with self.assertRaises(ValueError):tr.run(rows, evidence, [source], out)


if __name__ == '__main__':unittest.main()
