"""Read-only API reconciliation and real browser acceptance for product links."""
import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import requests
from playwright.sync_api import sync_playwright, expect


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--base', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args=parser.parse_args();base=args.base.rstrip('/')+'/';args.out.mkdir(parents=True,exist_ok=True)
    def get(action, ident=None):
        r=requests.get(base+'api.ashx',params={'action':action,**({'id':ident} if ident else {})},timeout=60)
        r.raise_for_status();return r.json()
    overview=get('overview')
    with ThreadPoolExecutor(3) as pool:
        products=list(pool.map(lambda i:get('products',i['IssuerId']),overview['issuers']))
    by_id={r['issuer']['IssuerId']:r for r in products}
    total=0
    for issuer in overview['issuers']:
        data=by_id[issuer['IssuerId']]
        assert issuer['NplProductCount']==data['total']==len(data['products'])
        assert len({r['TrustId'] for r in data['products']})==data['total']
        assert all(r['ProductSubject']==issuer['LegalName'] for r in data['products'])
        total+=data['total']
    assert total==863, total
    for issuer,trust in [('14','35325'),('24','35326'),('26','35327')]:
        assert any(r['TrustId']==trust for r in by_id[issuer]['products'])
    assert requests.get(base+'api.ashx',params={'action':'products','id':'999999999'},timeout=30).status_code==404
    assert requests.get(base+'api.ashx',params={'action':'products','id':'1 OR 1=1'},timeout=30).status_code==400
    errors=[];checks=['all_26_counts_match_product_lists','863_unique_subject_product_links','three_repaired_products_visible','invalid_ids_rejected']
    expect.set_options(timeout=60000)
    with sync_playwright() as p:
        browser=p.chromium.connect_over_cdp('http://127.0.0.1:18949')
        context=browser.contexts[0]
        page=context.new_page();page.set_viewport_size({'width':1440,'height':1000})
        page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(base)
        expect(page.locator('#overviewRows .product-count')).to_have_count(len(overview['issuers']))
        assert '待接入' not in page.locator('#overviewTable').inner_text()
        for issuer in overview['issuers']:
            expect(page.locator('[data-products="'+issuer['IssuerId']+'"]')).to_have_text(str(issuer['NplProductCount']))
        page.screenshot(path=str(args.out/'overview.png'),full_page=False)
        page.locator('[data-products="14"]').click()
        expect(page.locator('#productRows tr[data-trust-id]')).to_have_count(by_id['14']['total'])
        assert page.locator('#productRows tr').evaluate_all('(rows)=>rows.map(r=>r.dataset.trustId)')==[r['TrustId'] for r in by_id['14']['products']]
        expect(page.locator('#productSummary')).to_contain_text('累计 106 个项目')
        page.screenshot(path=str(args.out/'products.png'),full_page=False)
        page.locator('.product-table-scroll').evaluate('(el)=>el.scrollTop=el.scrollHeight')
        head=page.locator('.product-table thead').bounding_box();wrap=page.locator('.product-table-scroll').bounding_box()
        # Sticky cells remain at the container top even when the thead box scrolls.
        cell=page.locator('.product-table th').first.bounding_box()
        assert abs(cell['y']-wrap['y'])<3
        for status in ('Liquidated','Duration'):
            page.locator('#productStatus').select_option(status)
            expected=[r for r in by_id['14']['products'] if r['StatusTrust']==status]
            expect(page.locator('#productRows tr[data-trust-id]')).to_have_count(len(expected))
        checks+=['desktop_count_buttons','product_identity_and_order','status_filters','product_header_pinned']
        page.locator('#productsClose').click();expect(page.locator('#products')).not_to_be_visible()
        page.locator('[data-products="9"]').click()
        expect(page.locator('#productRows tr[data-trust-id]')).to_have_count(by_id['9']['total'])
        page.keyboard.press('Escape');expect(page.locator('#products')).not_to_be_visible()
        checks+=['paused_issuer_product_history_visible','keyboard_close']
        page.locator('#search').fill('建设银行')
        expect(page.locator('#overviewRows tr')).to_have_count(1)
        page.set_viewport_size({'width':390,'height':844})
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
        page.locator('[data-products="14"]').click()
        expect(page.locator('#productRows tr[data-trust-id]')).to_have_count(106)
        page.screenshot(path=str(args.out/'mobile-products.png'),full_page=False)
        box=page.locator('#products').bounding_box();scroll=page.locator('.product-table-scroll').bounding_box()
        assert box['x']>=0 and box['x']+box['width']<=391
        assert scroll['y']+scroll['height']<=box['y']+box['height']+1
        page.locator('.product-table-scroll').evaluate('(el)=>el.scrollLeft=el.scrollWidth')
        page.screenshot(path=str(args.out/'mobile-status.png'),full_page=False)
        page.locator('#productsClose').click();checks+=['mobile_no_page_overflow','mobile_dialog_fits','mobile_horizontal_scroll']
        page.set_viewport_size({'width':1440,'height':1000});page.locator('#search').fill('')
        page.locator('[data-issuer-row="30"] .issuer-link').click()
        expect(page.locator('#metricRows tr')).not_to_have_count(0)
        metric=page.locator('#metricRows [data-observation]').first;metric.click()
        expect(page.locator('#evidence')).to_be_visible();expect(page.locator('#evidenceBody')).not_to_be_empty()
        page.locator('#close').click();checks+=['financial_detail_and_evidence_still_work']
        page.goto(base);expect(page.locator('#overviewRows .product-count')).to_have_count(26)
        assert not errors, errors
        report={'base':base,'total_products':total,'issuers':len(overview['issuers']),'checks':checks,'page_errors':errors}
        (args.out/'acceptance.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        print(json.dumps(report,ensure_ascii=True))


if __name__=='__main__':main()
