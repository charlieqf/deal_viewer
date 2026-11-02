"""Exercise the live UI and save observations for operator acceptance, without a receipt."""
import argparse
import csv
import json
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import requests
from playwright.sync_api import sync_playwright, expect

BASE = 'https://r760.instmarket.com.au:1443/dealviewer/issuers/'
ROOT = Path(__file__).resolve().parents[2] / 'tmp/issuer-production'

def display(metric):
    value = Decimal(str(metric['NumericValue']))
    if metric['UnitCode'] == 'yuan':
        return f'{(value / Decimal(100000000)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP):,.2f}'
    return format(value.normalize(), 'f') + '%'


def verify_csv(path, data):
    names=dict(zip(['total_assets','capital_adequacy_ratio','nonperforming_loan_ratio','provision_coverage_ratio','operating_revenue','total_profit','net_profit','net_profit_attributable_to_parent'],['总资产','资本充足率','不良贷款率','拨备覆盖率','营业收入','利润总额','合并净利润','归母净利润']))
    periods={r['FinancialRecordId']:r['PeriodEnd'][:10] for r in data['records']}
    expected={(periods[m['FinancialRecordId']],names[m['MetricCode']]):m for m in data['metrics']}
    with path.open(encoding='utf-8-sig',newline='') as stream:rows=list(csv.DictReader(stream))
    assert len(rows)==len(periods)*len(names)
    assert len({(r['报告期'],r['指标']) for r in rows})==len(rows)
    for row in rows:
        metric=expected.get((row['报告期'],row['指标']),{})
        assert row['主体']==data['issuer']['LegalName']
        raw_label='原始入库值（文本，避免科学计数）' if '原始入库值（文本，避免科学计数）' in row else '原始入库值'
        assert row[raw_label].lstrip('\t')==str(metric.get('NumericValue') or '')
        for label,field in [('准则','AccountingBasisCode'),('缺项原因','MissingReason')]:
            assert row[label]==str(metric.get(field) or ''),(row['报告期'],row['指标'],label)
        units={'yuan':'元','percent':'%'}
        scopes={'accounting_consolidated':'会计集团','regulatory_consolidated':'监管并表','standalone':'本行 / 母公司','unknown':'未确认'}
        assert row['单位']==units.get(metric.get('UnitCode'),'')
        assert row['口径']==scopes.get(metric.get('EntityScopeCode'),'')
        if metric.get('ObservationId'):
            assert row['显示值']==display(metric)
        else:
            note=json.loads(metric.get('ReviewNote') or '{}')
            missing={'material_label_not_found':'待补充/核查材料','scope_pending':'口径待核对','numeric_evidence_pending':'数字待核对','basis_pending':'准则待核对','evidence_pending':'证据待核对'}
            assert row['显示值']==(missing.get(note.get('missing_evidence',{}).get('code'),'待识别') if metric else '尚未采集')
        assert row['复核状态']=={'pending':'待复核','verified':'已核对','corrected':'已更正'}.get(metric.get('ReviewStatus'),'待复核')
        assert row['主体采用状态']==('暂停采用' if data['issuer']['MonitoringStatus']=='paused' else '待业务核准')
    return len(rows)

def open_native_pdf(context,link,document_id,page_number,screenshot):
    with context.expect_page() as opened:link.click()
    source=opened.value
    source.wait_for_url('**api.ashx?action=document&id=**',wait_until='domcontentloaded',timeout=60000)
    assert f'id={document_id}#page={page_number}' in source.url
    response=requests.get(source.url.split('#')[0],headers={'Range':'bytes=0-511'},timeout=45)
    assert response.status_code==206 and response.headers['Content-Type']=='application/pdf'
    assert response.content.startswith(b'%PDF')
    # The native browser PDF viewer has no application-owned per-page canvas.
    # Capture it for the same operator visual acceptance as the prior viewer.
    source.wait_for_timeout(15000)
    source.screenshot(path=str(screenshot))
    return source

def main():
    expect.set_options(timeout=45000)
    parser = argparse.ArgumentParser()
    parser.add_argument('--slug', required=True)
    parser.add_argument('--issuer-id', required=True)
    parser.add_argument('--values-only', action='store_true', help='Recheck changed values and export after a prior full browser acceptance')
    args = parser.parse_args()
    out = ROOT / args.slug
    out.mkdir(parents=True, exist_ok=True)
    data = requests.get(BASE + 'api.ashx', params={'action': 'issuer', 'id': args.issuer_id}, timeout=45).json()
    sql = json.loads((out/'sql-readback.json').read_text(encoding='utf-8'))
    api_metrics = {(m['FinancialRecordId'],m['MetricCode']):m for m in data['metrics']}
    sql_metrics = {(m['FinancialRecordId'],m['MetricCode']):m for m in sql['metrics']}
    assert api_metrics.keys()==sql_metrics.keys(), 'API/SQL selected records differ'
    fields=('ObservationId','NumericValue','ValueStatus','UnitCode','EntityScopeCode','AccountingBasisCode','ReviewStatus')
    assert all(all(str(api_metrics[k].get(f))==str(sql_metrics[k].get(f)) for f in fields) for k in api_metrics), 'API/SQL metric mismatch'
    assert sql['published_count']==0
    (out/'live-detail.json').write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    observations = {'issuer': data['issuer'], 'checked_at': datetime.now(timezone.utc).isoformat(), 'browser': 'Chromium / independent Playwright', 'checks': ['all_selected_metrics_match_sql_readback'], 'evidence': []}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, timeout=30000)
        context = browser.new_context(viewport={'width': 1500, 'height': 1100})
        page = context.new_page()
        page.set_default_timeout(45000)
        page.goto(BASE, wait_until='domcontentloaded')
        card = page.locator(f'#overviewRows a[href="#issuer={args.issuer_id}"]')
        expect(card).to_contain_text(data['issuer']['LegalName'])
        observations['overview'] = card.inner_text()
        card.click()
        expect(page.locator('#name')).to_have_text(data['issuer']['LegalName'])
        expect(page.locator('#metricRows tr')).to_have_count(len(data['records']))
        for metric in data['metrics']:
            if metric.get('ObservationId') and metric['ValueStatus'] == 'reported':
                expect(page.locator(f'#metricRows button[data-observation="{metric["ObservationId"]}"]')).to_have_text(display(metric))
                if metric['EntityScopeCode'] in ('unknown','standalone'):
                    label='口径待确认' if metric['EntityScopeCode']=='unknown' else '本行 / 母公司口径'
                    expect(page.locator(f'#metricRows button[data-observation="{metric["ObservationId"]}"]').locator('..').locator('.scope-pending')).to_contain_text(label)
        expect(page.locator('#detailStatus')).to_have_text(f'{len(data["documents"])} 份报告 · {len(data["records"])} 个报告期')
        observations['checks'].append('overview_and_all_displayed_values_match_read_api')
        page.screenshot(path=str(out/'browser-metrics.png'), full_page=True)
        print('UI values verified: '+args.slug, flush=True)
        years = sorted({r['PeriodEnd'][:4] for r in data['records']}, reverse=True)
        selected_year = '2025' if '2025' in years else years[0]
        page.locator('#year').select_option(selected_year)
        expect(page.locator('#metricRows tr')).to_have_count(sum(r['PeriodEnd'].startswith(selected_year) for r in data['records']))
        page.reload(wait_until='domcontentloaded')
        expect(page.locator('#year')).to_have_value(selected_year)
        expect(page.locator('#metricRows tr')).to_have_count(sum(r['PeriodEnd'].startswith(selected_year) for r in data['records']))
        page.locator('#year').select_option('')
        observations['checks'].append('year_filter_and_reload_preserve_state')
        with page.expect_download() as event:
            page.locator('#csv').click()
        event.value.save_as(str(out/'browser-export.csv'))
        observations['checks'].append('csv_download')
        observations['csv_rows_verified']=verify_csv(out/'browser-export.csv',data)
        observations['checks'].append('csv_values_scopes_basis_and_review_status_match_api')
        if args.values_only:
            observations['reported']=sum(m['ValueStatus']=='reported' for m in data['metrics'])
            observations['unresolved']=sum(m['ValueStatus']!='reported' for m in data['metrics'])
            (out/'browser-delta-observations.json').write_text(json.dumps(observations,ensure_ascii=False,indent=2),encoding='utf-8')
            browser.close()
            print(json.dumps(observations,ensure_ascii=False),flush=True)
            return
        for kind, codes in [('amount', ['total_assets', 'operating_revenue', 'total_profit']), ('ratio', ['capital_adequacy_ratio', 'nonperforming_loan_ratio', 'provision_coverage_ratio']), ('profit',['net_profit','net_profit_attributable_to_parent'])]:
            candidates = [m for m in data['metrics'] if m.get('ObservationId') and m['MetricCode'] in codes and m['ValueStatus'] == 'reported']
            candidates.sort(key=lambda m: (m['PeriodEnd'][:10] != '2025-12-31', codes.index(m['MetricCode']), m['PeriodEnd']), reverse=False)
            if not candidates:
                observations['evidence'].append({'kind': kind, 'unavailable': True})
                continue
            metric = candidates[0]
            page.locator(f'#metricRows button[data-observation="{metric["ObservationId"]}"]').click()
            expect(page.locator('#evidenceItems .evidence-item')).not_to_have_count(0)
            text = page.locator('#evidenceBody').inner_text()
            page.locator('#evidence').screenshot(path=str(out/f'browser-evidence-{kind}.png'))
            entries = requests.get(BASE+'api.ashx', params={'action': 'evidence', 'id': metric['ObservationId']}, timeout=45).json()
            value_evidence = next(e for e in entries if e['EvidenceRole'] == 'value')
            link = page.locator('#evidenceItems a').filter(has_text=value_evidence['Title']).first
            href = f'api.ashx?action=document&id={value_evidence["DocumentId"]}#page={value_evidence["PdfPage"]}'
            link = page.locator(f'#evidenceItems a[href="{href}"]').first
            source=open_native_pdf(context,link,value_evidence['DocumentId'],value_evidence['PdfPage'],out/f'browser-source-{kind}.png')
            document_url=source.url.split('#')[0]
            observations['evidence'].append({'kind': kind, 'metric': metric, 'dialog': text, 'source_url': source.url, 'pdf_url': document_url, 'entries': entries})
            source.close()
            page.locator('#close').click()
        page.locator('[data-jump=documentTitle]').click()
        expect(page.locator('#documents tr')).to_have_count(len(data['documents']))
        observations['checks'].append('document_list_and_original_pdf_open')
        page.locator('#tasksTab').click()
        observations['tasks_text'] = page.locator('#tasks').inner_text()
        page.locator('#metricsTab').click()
        missing = page.locator('#metricRows details.missing')
        observations['missing_count'] = missing.count()
        if missing.count():
            missing.first.locator('summary').click()
            observations['missing_example'] = missing.first.inner_text()
            assert missing.first.locator('small').first.inner_text().strip()
            page.screenshot(path=str(out/'browser-missing.png'), full_page=True)
            observations['checks'].append('missing_reason_expands_without_zero_fill')
        observations['checks'].append('selected_amount_ratio_evidence_and_pdf_physical_pages')
        browser.close()
    (out/'browser-observations.json').write_text(json.dumps(observations, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'slug': args.slug, 'issuer': data['issuer'], 'checks': observations['checks'], 'evidence': [{'kind': e['kind'], 'metric': e.get('metric',{}).get('MetricCode'), 'value': e.get('metric',{}).get('NumericValue'), 'url': e.get('source_url')} for e in observations['evidence']], 'missing': observations['missing_count'], 'operator_review_required': True}, ensure_ascii=False), flush=True)

if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    main()
