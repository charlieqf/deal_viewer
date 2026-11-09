"""Read-only browser acceptance of the shared status contract (no review writes)."""
import argparse
import copy
import json
from pathlib import Path
from urllib.request import urlopen

from playwright.sync_api import sync_playwright, expect
from preinvestment import item_statuses, SOURCES
from risk_indicators import attach_indicators


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    base = args.base.rstrip('/') + '/'
    with urlopen(base + 'api.ashx?action=issuers', timeout=60) as response:
        issuer = json.load(response)[0]['IssuerId']
    with urlopen(base + 'api.ashx?action=risk&id=' + str(issuer), timeout=60) as response:
        live = json.load(response)
    errors, checked = [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        page.on('pageerror', lambda error: errors.append(str(error)))
        # First validate the real response without interception.
        page.goto(base + 'risk.html?id=' + str(issuer))
        expect(page.locator('#preItems tr[data-category]')).to_have_count(3, timeout=60000)
        for kind, item in live['preinvestment_items'].items():
            expect(page.locator(f'tr[data-category="{kind}"]')).to_have_attribute('data-status', item['status'])
        checked.append('live_api_and_page_agree')
        args.out.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(args.out / 'live-desktop.png'), full_page=True)

        def scenario(name, mutate, expected, requires_review=False):
            fixture = copy.deepcopy(live)
            fixture['events'] = []
            fixture['preinvestment_summary'].update(SuspensionDeduction=0, ServicerDeduction=0,
                                                     MaturityDeduction=0, SuggestedDeduction=0)
            fixture['sources'] = [dict(code=code, label=code, success=None, latest=dict(
                Status='success', CheckedAt='2026-11-06 03:00:00', QueryStart='2024-11-06',
                QueryEnd='2026-11-06', PendingDocuments=0))
                for codes in SOURCES.values() for code in codes]
            mutate(fixture)
            fixture['preinvestment_items'] = item_statuses(fixture['events'], fixture['sources'], fixture['preinvestment_summary'])
            attach_indicators(fixture)
            pattern = '**/api.ashx?action=risk&*'
            page.route(pattern, lambda route: route.fulfill(json=fixture))
            page.reload()
            row = page.locator('tr[data-category="abs_suspension"]')
            expect(row).to_have_attribute('data-status', expected, timeout=60000)
            expect(row).to_contain_text(fixture['preinvestment_items']['abs_suspension']['status_label'])
            if expected == 'not_detected':
                expect(row).to_contain_text('无需人工操作')
                expect(row).to_contain_text('查询截至 2026-11-06')
            else:
                expect(row).not_to_contain_text('无需人工操作')
            if requires_review:
                expect(row).to_contain_text('可点击查看明细核对')
                row.locator('button').click()
                expect(page.locator('#events button')).to_have_text('核对 / 更正')
                page.locator('#closeIndicator').click()
            else:
                expect(row).not_to_contain_text('可点击查看明细核对')
            page.unroute(pattern)
            checked.append(name)

        scenario('empty_success_no_action', lambda f: None, 'not_detected')
        scenario('missing_source_not_clear', lambda f: f['sources'].pop(0), 'not_queried')
        scenario('failed_source_not_review', lambda f: f['sources'][0]['latest'].update(Status='failed'), 'query_failed')
        scenario('backlog_not_review', lambda f: f['sources'][0]['latest'].update(PendingDocuments=2), 'pending_processing')
        def pending(fixture):
            fixture['events'] = [dict(Category='abs_suspension', Status='pending', EventDate=None,
                                      RiskEventId=1, FieldsJson='{}', EvidenceJson='[]')]
        scenario('concrete_pending_event_review_link', pending, 'pending_review', True)
        page.set_viewport_size({'width': 390, 'height': 844})
        page.reload()
        expect(page.locator('#preItems tr[data-category]')).to_have_count(3, timeout=60000)
        page.screenshot(path=str(args.out / 'live-mobile.png'), full_page=True)
        assert not errors, errors
        browser.close()
    result = dict(passed=True, checks=checked, page_errors=errors)
    (args.out / 'browser-status-acceptance.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
