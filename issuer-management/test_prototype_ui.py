"""Browser acceptance for the prototype layout against actual selected data."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import requests
from playwright.sync_api import sync_playwright,expect
from production_browser_check import display,verify_csv,open_native_pdf
from urllib.parse import urlparse,parse_qs

def check_original(context,link,screenshot):
    url=urlparse(link.get_attribute('href'))
    document_id=parse_qs(url.query)['id'][0]
    page_number=parse_qs(url.fragment).get('page',['1'])[0]
    return open_native_pdf(context,link,document_id,page_number,screenshot)

SOURCE=Path(__file__).parent
FIELDS=('ObservationId','NumericValue','ValueStatus','UnitCode','EntityScopeCode','AccountingBasisCode','MeasurementBasisCode','ReviewStatus','MissingReason')

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--base',required=True);p.add_argument('--out',type=Path,required=True)
    p.add_argument('--local-assets',action='store_true')
    p.add_argument('--compare-base')
    p.add_argument('--legacy-html',type=Path)
    a=p.parse_args();base=a.base.rstrip('/')+'/';a.out.mkdir(parents=True,exist_ok=True)
    def get(action,id=None,origin=base):
        r=requests.get(origin+'api.ashx',params={'action':action,**({'id':id} if id else {})},timeout=60)
        r.raise_for_status();return r.json()
    overview=get('overview');issuers=overview['issuers']
    with ThreadPoolExecutor(3) as pool:
        details=list(pool.map(lambda x:get('issuer',x['IssuerId']),issuers))
    data={d['issuer']['IssuerId']:d for d in details}
    for d in details:
        id=d['issuer']['IssuerId'];r=d['records'][0]
        latest=next(x for x in overview['records'] if x['IssuerId']==id)
        assert latest['FinancialRecordId']==r['FinancialRecordId']
        expected={m['MetricCode']:m for m in d['metrics'] if m['FinancialRecordId']==r['FinancialRecordId']}
        actual={m['MetricCode']:m for m in overview['metrics'] if m['IssuerId']==id}
        assert actual.keys()==expected.keys()
        for code,m in expected.items():
            assert all(actual[code].get(f)==m.get(f) for f in FIELDS)
    if a.compare_base:
        with ThreadPoolExecutor(3) as pool:
            previous=list(pool.map(lambda x:get('issuer',x['IssuerId'],a.compare_base.rstrip('/')+'/'),issuers))
        for d,old in zip(details,previous):
            for key in ('issuer','records','metrics','tasks'):assert d[key]==old[key],(d['issuer']['LegalName'],key)
            assert [{k:v for k,v in doc.items() if k!='PublishedDate'} for doc in d['documents']]==old['documents']
    (a.out/'api-snapshot.json').write_text(json.dumps({'overview':overview,'details':details},ensure_ascii=False),encoding='utf-8')
    checks=['overview_batch_matches_all_latest_records','no_metric_changes']
    errors=[];csv_count=0;selected=0
    expect.set_options(timeout=60000)
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=False)
        context=browser.new_context(viewport={'width':1440,'height':1000},accept_downloads=True)
        page=context.new_page();page.on('pageerror',lambda e:errors.append(str(e)))
        if a.local_assets:
            for filename in ('index.html','style.css','app.js'):
                path=SOURCE/'web'/filename
                mime={'index.html':'text/html','style.css':'text/css','app.js':'text/javascript'}[filename]
                page.route('**/'+filename+'*',lambda route,request,path=path,mime=mime:route.fulfill(body=path.read_bytes(),content_type=mime))
            page.route(base,lambda route:route.fulfill(body=(SOURCE/'web/index.html').read_bytes(),content_type='text/html'))
        page.goto(base)
        expect(page.locator('#overviewRows tr')).to_have_count(len(issuers))
        for m in overview['metrics']:
            if m['ObservationId']:
                expect(page.locator(f'#overviewRows button[data-observation="{m["ObservationId"]}"]')).to_have_text(display(m))
        page.screenshot(path=str(a.out/'overview.png'),full_page=True)
        page.locator('#search').fill('兴业')
        expect(page.locator('#overviewRows tr')).to_have_count(1)
        page.locator('#search').fill('不存在的主体')
        expect(page.locator('#overviewRows')).to_contain_text('没有找到')
        page.locator('#search').fill('')
        page.locator('#type').select_option('amc')
        expect(page.locator('#overviewRows tr')).to_have_count(sum(x['InstitutionType']=='amc' for x in issuers))
        page.locator('#type').select_option('')
        page.locator('#adoption').select_option('paused')
        expect(page.locator('#overviewRows tr')).to_have_count(1)
        expect(page.locator('#overviewRows')).to_contain_text('暂停采用')
        page.locator('#adoption').select_option('')
        # Overview evidence preserves the list route and goes to the real PDF page.
        m=next(m for m in overview['metrics'] if m['IssuerId']=='19' and m['MetricCode']=='total_assets')
        page.locator(f'#overviewRows button[data-observation="{m["ObservationId"]}"]').click()
        expect(page.locator('#evidenceItems .evidence-item').first).to_be_visible()
        viewer=check_original(context,page.locator('#evidenceItems a').first,a.out/'evidence-original.png')
        viewer.close();page.locator('#close').click()
        assert 'issuer=' not in page.url
        checks+=['overview_search_type_and_pause_filters','overview_evidence_original_pdf']
        for d in details:
            id=d['issuer']['IssuerId']
            page.goto(base+'#issuer='+id)
            expect(page.locator('#name')).to_have_text(d['issuer']['LegalName'])
            expect(page.locator('#metricRows tr')).to_have_count(len(d['records']))
            for m in d['metrics']:
                if m['ObservationId'] and m['ValueStatus']=='reported':
                    expect(page.locator(f'#metricRows button[data-observation="{m["ObservationId"]}"]')).to_have_text(display(m))
                    selected+=1
            latest=d['records'][0]['FinancialRecordId']
            for m in d['metrics']:
                if m['FinancialRecordId']==latest and m['MetricCode'] in ('total_assets','capital_adequacy_ratio','nonperforming_loan_ratio','provision_coverage_ratio') and m['ObservationId']:
                    expect(page.locator(f'#kpis button[data-observation="{m["ObservationId"]}"]')).to_have_text(display(m))
            expect(page.locator('#documents tr')).to_have_count(len(d['documents']))
            with page.expect_download() as event:page.locator('#csv').click()
            target=a.out/f'issuer-{id}.csv';event.value.save_as(str(target));csv_count+=verify_csv(target,d)
            assert page.locator('#sourceHold').is_visible()==(d['issuer']['MonitoringStatus']=='paused')
            if id in ('8','9','19','26'):
                page.screenshot(path=str(a.out/f'detail-{id}.png'),full_page=True)
            print(json.dumps({'issuer':id,'values_verified':sum(bool(m['ObservationId']) for m in d['metrics'])}),flush=True)
        checks+=['all_26_history_tables_and_latest_cards_match_api','all_26_csv_contents_verified','all_document_lists_complete','paused_status_preserved']
        # The user-reported Bohai omissions are now values with explicit bank scope.
        bohai=data['30'];record=next(r for r in bohai['records'] if r['PeriodEnd'].startswith('2025-06-30'))
        for code,value in [('nonperforming_loan_ratio','1.810000'),('provision_coverage_ratio','159.700000')]:
            m=next(m for m in bohai['metrics'] if m['FinancialRecordId']==record['FinancialRecordId'] and m['MetricCode']==code)
            assert m['NumericValue']==value and m['EntityScopeCode']=='standalone' and m['ReviewStatus']=='pending'
        page.goto(base+'#issuer=30');expect(page.locator('#name')).to_contain_text('渤海')
        for code in ('nonperforming_loan_ratio','provision_coverage_ratio'):
            m=next(m for m in bohai['metrics'] if m['FinancialRecordId']==record['FinancialRecordId'] and m['MetricCode']==code)
            cell=page.locator(f'#metricRows button[data-observation="{m["ObservationId"]}"]').locator('..')
            expect(cell).to_contain_text('本行 / 母公司口径')
        missing=page.locator('#metricRows details.missing').filter(has=page.locator('a')).first
        missing.locator('summary').click()
        expect(missing.locator('..')).to_contain_text('核对并录入')
        viewer=check_original(context,missing.locator('a').first,a.out/'missing-original.png')
        viewer.close()
        page.screenshot(path=str(a.out/'bohai-help-and-correction.png'),full_page=True)
        checks+=['bohai_reported_values_with_bank_scope','missing_value_action_and_original_page_link']
        page.goto(base+'#issuer=19');expect(page.locator('#name')).to_contain_text('兴业')
        page.locator('#year').select_option('2025')
        expect(page.locator('#metricRows tr')).to_have_count(4)
        with page.expect_download() as event:page.locator('#csv').click()
        event.value.save_as(str(a.out/'year-2025.csv'))
        import csv
        with (a.out/'year-2025.csv').open(encoding='utf-8-sig',newline='') as f:
            yearrows=list(csv.DictReader(f));assert len(yearrows)==32 and all(r['报告期'].startswith('2025') for r in yearrows)
        page.reload();expect(page.locator('#year')).to_have_value('2025')
        page.locator('#metricSelect').select_option('net_profit')
        assert page.locator('#trendChart a').count()==3
        expect(page.locator('#chartCaption')).to_contain_text('历年累计值')
        page.locator('#trendChart a').last.click()
        expect(page.locator('#evidenceTitle')).to_contain_text('合并净利润')
        expect(page.locator('#evidenceItems .evidence-item').first).to_be_visible()
        page.keyboard.press('Escape')
        page.reload();expect(page.locator('#metricSelect')).to_have_value('net_profit')
        page.locator('[data-doc-filter=rating]').click()
        expect(page.locator('#documents')).to_contain_text('尚未接入')
        page.locator('[data-doc-filter=all]').click()
        page.locator('#ratingsTab').click();expect(page.locator('#ratingsPanel')).to_be_visible()
        page.keyboard.press('ArrowRight');expect(page.locator('#tasksPanel')).to_be_visible()
        page.keyboard.press('Home');expect(page.locator('#metricsPanel')).to_be_visible()
        # Unknowns/conflicts must not become comparable solid segments or calculated changes.
        guards=page.evaluate("""() => {
          const a={ObservationId:'1',ValueStatus:'reported',NumericValue:'100',MetricCode:'total_assets',UnitCode:'yuan',EntityScopeCode:'accounting_consolidated',AccountingBasisCode:'CAS',MeasurementBasisCode:'not_applicable'};
          return [comparable(a,{...a,ObservationId:'2'}),comparable(a,{...a,EntityScopeCode:'standalone'}),comparable(a,{...a,AccountingBasisCode:'unknown'}),comparable({...a,AccountingBasisCode:'unknown'},{...a,AccountingBasisCode:'unknown'}),comparable(a,{...a,NumericValue:null}),comparable(a,{...a,ReviewNote:'{"conflict":true}'})];
        }""")
        assert guards==[True,False,False,False,False,False]
        checks+=['year_filter_reload_and_filtered_csv','flow_chart_same_quarter_years','chart_evidence','chart_comparability_guards','tabs_keyboard_and_rating_empty_states']
        # Download the actual archived PDF and compare it with its registered hash.
        doc=data['19']['documents'][0]
        with page.expect_download(timeout=90000) as event:
            page.locator(f'#documents a[download][href*="id={doc["DocumentId"]}"]').click()
        target=a.out/'sample-original.pdf';event.value.save_as(str(target))
        assert hashlib.sha256(target.read_bytes()).hexdigest()==doc['Sha256'].lower()
        checks.append('original_download_hash_matches_registered_pdf')
        page.goto(base+'#issuer=26');expect(page.locator('#name')).to_contain_text('招商')
        page.screenshot(path=str(a.out/'detail-desktop.png'),full_page=True)
        for width in (390,768):
            page.set_viewport_size({'width':width,'height':844})
            page.screenshot(path=str(a.out/f'detail-{width}.png'),full_page=True)
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
            assert page.locator('#metricSelect').bounding_box()['height']<60
        page.goto(base);expect(page.locator('#overviewRows tr')).to_have_count(26)
        page.set_viewport_size({'width':390,'height':844})
        page.screenshot(path=str(a.out/'overview-mobile.png'),full_page=True)
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        checks.append('mobile_and_tablet_no_page_overflow')
        if a.legacy_html:
            old= a.legacy_html.read_text(encoding='utf-8')
            import re
            old=re.sub(r'app.js\?v=[^"]+','app.js?v=20261020-16',old)
            page.route(base,lambda route:route.fulfill(body=old,content_type='text/html'),times=1)
            page.goto(base+'#issuer=19')
            expect(page.locator('#overviewRows')).to_have_count(1)
            expect(page.locator('#metricRows tr')).to_have_count(10)
            expect(page.locator('#error')).to_be_hidden()
            checks.append('previous_html_current_script_shell_recovery')
        assert not errors,errors
        browser.close()
    result={'checked_at':datetime.now(timezone.utc).isoformat(),'base':base,'issuers':len(details),'selected_values_checked':selected,'csv_rows_checked':csv_count,'checks':checks,'page_errors':errors,'business_review':'pending'}
    (a.out/'acceptance.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False),flush=True)

if __name__=='__main__':main()
