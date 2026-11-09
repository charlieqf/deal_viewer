"""Read-only live UI acceptance plus intercepted scenarios; never writes real data."""
import argparse
import copy
import json
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright, expect
from preinvestment import item_statuses
from risk_indicators import attach_indicators, INDICATORS
from test_risk_indicators import fixture


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base', required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--local', action='store_true')
    args = parser.parse_args()
    base = args.base.rstrip('/') + '/'
    args.out.mkdir(parents=True, exist_ok=True)
    with urlopen(base + 'api.ashx?action=issuers', timeout=45) as response:
        issuers = json.load(response)
    issuer = next((x for x in issuers if x['LegalName'] == '招商银行股份有限公司'), issuers[0])
    with urlopen(base + 'api.ashx?action=risk&id=' + str(issuer['IssuerId']), timeout=45) as response:
        live = json.load(response)
    with urlopen(base + 'health', timeout=45) as response:
        health = json.load(response)
    if args.local:
        attach_indicators(live)
    errors, checks, posted = [], [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width':1440, 'height':1100})
        page.on('pageerror', lambda error: errors.append(str(error)))
        if args.local:
            for filename in ('risk.html', 'risk.js', 'risk.css', 'preinvestment.js'):
                page.route('**/' + filename + '*', lambda route: route.fulfill(
                    path=str(Path(__file__).parent / 'web' / Path(urlsplit(route.request.url).path).name)))
            page.route('**/api.ashx?action=risk&*', lambda route: route.fulfill(json=live))
        # Guard all write requests, including accidental writes in the live phase.
        def reject_write(route):
            raise AssertionError('Unexpected real review request')
        page.route('**/api.ashx?action=review', reject_write)
        page.goto(base + 'risk.html?id=' + str(issuer['IssuerId']))
        expect(page.locator('#title')).to_contain_text(issuer['LegalName'], timeout=45000)
        expect(page.locator('#title')).to_contain_text('风险信息')
        expect(page.locator('#checkWindow')).to_contain_text('业务日期')
        expect(page.locator('[data-indicator="pre_a"]')).to_have_text('查看明细')
        expect(page.locator('.indicator-table tbody tr')).to_have_count(6)
        assert page.locator('h2').all_text_contents()[:2] == ['投前评价指标', '投后评价指标']
        for term in ('建议分', '建议扣分', '建议评分'):
            assert term not in page.locator('body').inner_text()
        expect(page.locator('#score, #preScore')).to_have_count(0)
        for item in live['indicators']:
            row = page.locator(f'tr[data-category="{item["category"]}"]')
            expect(row).to_contain_text(item['label'])
            expect(row).to_have_attribute('data-status', item['status'])
            row.locator('button').click()
            expect(page.locator('#indicatorDescription')).to_have_text(item['label'])
            expect(page.locator('#indicatorDialog')).to_be_visible()
            assert page.locator('#indicatorDialog [data-event]').count() == item['event_count']
            page.locator('#closeIndicator').click()
        checks += ['live_six_indicators_order_and_labels', 'no_scores', 'six_correct_dialogs']
        page.screenshot(path=str(args.out / 'risk-desktop.png'), full_page=True)
        page.set_viewport_size({'width':390, 'height':844})
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        page.locator('[data-indicator="post_a"]').click()
        expect(page.locator('#indicatorDescription')).to_have_text(live['indicators'][3]['label'])
        page.screenshot(path=str(args.out / 'risk-mobile-dialog.png'), full_page=True)
        page.locator('#closeIndicator').click()
        page.screenshot(path=str(args.out / 'risk-mobile.png'), full_page=True)
        checks += ['mobile_no_page_overflow_and_dialog']
        page.set_viewport_size({'width':1440, 'height':1100})

        # Controlled events exercise all categories, excluded history and editing.
        events = [dict(RiskEventId=i, Category=definition[4], Status='pending', EventDate=None,
                       FieldsJson='{}', EvidenceJson=json.dumps([dict(document_id=10, sha256='abc',
                       source_url='https://example.com/notice', evidence={'conclusion':dict(page=2,quote='核查原文')})]),
                       Notes='<img src=x onerror=alert(1)>', RowVersion='0011')
                  for i, definition in enumerate(INDICATORS, 1)]
        events.append(dict(events[3], RiskEventId=7, Status='below_threshold'))
        events.append(dict(events[4], RiskEventId=8, Status='confirmed', EventDate='2024-11-08'))
        data = fixture(events)
        for source in data['sources']:
            source.update(success=None)
        attach_indicators(data)
        page.unroute('**/api.ashx?action=risk&*')
        page.route('**/api.ashx?action=risk&*', lambda route: route.fulfill(json=data))
        page.reload()
        expect(page.locator('tr[data-category="penalty"]')).to_have_attribute('data-status', 'pending_review')
        for item in data['indicators']:
            page.locator('[data-indicator="'+item['code']+'"]').click()
            expect(page.locator('#events .event-indicator').first).to_contain_text(item['label'])
            expect(page.locator('#events')).to_contain_text('待确认')
            assert page.locator('#events img').count() == 0
            link = page.locator('#events a').first
            assert 'action=document&id=10' in link.get_attribute('href')
            assert link.get_attribute('target') == '_blank'
            page.locator('#events [data-event]').first.click()
            expect(page.locator('#editIndicator')).to_contain_text(item['label'])
            expect(page.locator('#category')).to_have_value(item['category'])
            page.locator('#cancel').click()
            if item['code'] == 'post_a':
                page.locator('#excludedDetails > summary').click()
                expect(page.locator('#collapsed')).to_contain_text('未达阈值')
            if item['code'] == 'post_b':
                expect(page.locator('#indicatorStatus')).to_contain_text('历史已确认 1 项')
            page.locator('#closeIndicator').click()
        checks += ['exact_event_classification', 'review_indicator_context', 'original_links', 'escaped_content', 'excluded_and_historical_events']

        def review(route):
            payload = route.request.post_data_json
            posted.append(payload)
            assert payload['operation'] == 'risk_event' and payload['id'] == 5
            event = next(e for e in data['events'] if e['RiskEventId'] == 5)
            event.update(Status=payload['status'], EventDate=payload['event_date'], FieldsJson=json.dumps(payload['fields']))
            attach_indicators(data)
            route.fulfill(json={'saved':True})
        page.unroute('**/api.ashx?action=review')
        page.route('**/api.ashx?action=review', review)
        page.locator('[data-indicator="post_b"]').click()
        page.locator('#events [data-event="5"]').click()
        page.locator('#status').select_option('confirmed')
        page.locator('#eventDate').fill('2026-11-09')
        page.locator('#direction').select_option('下调')
        page.locator('#reason').fill('模拟响应验证，不写真实数据库')
        page.locator('#reviewer').fill('页面测试')
        page.locator('#key').fill('mock-only')
        page.locator('#save').click()
        expect(page.locator('#editor')).not_to_be_visible()
        expect(page.locator('#indicatorStatus strong')).to_have_text('已确认相关风险事件')
        expect(page.locator('tr[data-category="rating"]')).to_have_attribute('data-status', 'confirmed')
        assert len(posted) == 1
        page.locator('#closeIndicator').click()
        checks += ['mock_review_refreshes_row_and_dialog']

        for state, expected in [('failed', 'query_failed'), ('not_connected', 'not_connected'),
                                ('not_queried', 'not_queried'), ('success', 'pending_processing')]:
            data = fixture()
            source = next(s for s in data['sources'] if s['code'] == 'chinamoney_default')
            source['latest'].update(Status=state, PendingDocuments=2)
            attach_indicators(data)
            page.reload()
            row = page.locator('tr[data-category="default"]')
            expect(row).to_have_attribute('data-status', expected)
            expect(row).not_to_contain_text('无需人工操作')
        checks += ['failed_missing_backlog_not_clear']
        assert not errors, errors
        browser.close()
    result = dict(passed=True, checks=checks, page_errors=errors, real_writes=0,
                  live=not args.local, commit=health['commit'])
    (args.out / 'acceptance.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
