"""Real Chromium reads and writes only the dedicated acceptance database."""
import argparse,hashlib,json
from pathlib import Path
from playwright.sync_api import sync_playwright,expect
import requests


def main():
    p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--config',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    a=p.parse_args();cfg=json.loads(a.config.read_text());base=a.base.rstrip('/')+'/';a.out.mkdir(parents=True,exist_ok=True)
    errors=[];checks=[]
    with sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True);context=browser.new_context(accept_downloads=True);page=context.new_page()
        page.on('pageerror',lambda e:errors.append(str(e)));page.set_viewport_size({'width':1440,'height':1100})
        page.goto(base+'risk.html?id='+str(cfg['issuer_id']))
        expect(page.locator('#title')).to_contain_text('页面验收银行',timeout=45000)
        expect(page.locator('#score strong')).to_have_text('15 / 15')
        expect(page.locator('#events')).to_contain_text('待确认')
        assert page.locator('#sources .red').count()>=1;checks+=['pending_not_scored','stale_red','source_not_connected']
        page.locator('[data-event="'+str(cfg['event_id'])+'"]').click()
        page.locator('#status').select_option('confirmed');page.locator('#reason').fill('浏览器核对主体评级下调及原文')
        page.locator('#reviewer').fill('隔离库浏览器验收');page.locator('#key').fill(cfg['review_key']);page.locator('#save').click()
        expect(page.locator('#editor')).not_to_be_visible(timeout=45000);expect(page.locator('#score strong')).to_have_text('5 / 15',timeout=45000)
        checks+=['single_reviewer_confirm','current_score_updates']
        page.screenshot(path=str(a.out/'risk-desktop.png'),full_page=True)
        page.reload();expect(page.locator('#score strong')).to_have_text('5 / 15',timeout=45000)
        page.locator('[data-event="'+str(cfg['event_id'])+'"]').click();page.locator('#status').select_option('excluded')
        page.locator('#reason').fill('浏览器更正归类，保留原文及日志');page.locator('#reviewer').fill('隔离库浏览器验收');page.locator('#key').fill(cfg['review_key']);page.locator('#save').click()
        try:expect(page.locator('#editor')).not_to_be_visible(timeout=45000)
        except AssertionError:raise AssertionError(page.locator('#formError').inner_text())
        expect(page.locator('#score strong')).to_have_text('15 / 15',timeout=45000)
        expect(page.locator('#events [data-event]')).to_have_count(0)
        page.locator('#collapsed').locator('..').locator('summary').first.click();expect(page.locator('#collapsed')).to_contain_text('已排除')
        checks+=['correction_to_excluded','collapsed_exclusions']
        with page.expect_download() as download:
            page.evaluate("url=>{const a=document.createElement('a');a.href=url;a.download='original.pdf';document.body.append(a);a.click();}",base+'api.ashx?action=document&id='+str(cfg['document_id']))
        path=a.out/'original.pdf';download.value.save_as(path);assert hashlib.sha256(path.read_bytes()).hexdigest()==cfg['digest'];checks.append('original_download_hash')
        page.set_viewport_size({'width':390,'height':844});page.screenshot(path=str(a.out/'risk-mobile.png'),full_page=True)
        page.goto(base+'risk.html?mapping=1');expect(page.locator('#mappings table')).to_be_visible(timeout=45000)
        page.locator('[data-mapping]').first.click();page.locator('#mappingIssuer').select_option(str(cfg['issuer_id']));page.locator('#reason').fill('浏览器按原文更正发起人');page.locator('#reviewer').fill('隔离库浏览器验收');page.locator('#key').fill(cfg['review_key']);page.locator('#save').click();expect(page.locator('#editor')).not_to_be_visible(timeout=45000);checks.append('mapping_correction')
        page.goto(base+'#issuer='+str(cfg['issuer_id']));expect(page.locator('#issuerRiskLink')).to_be_visible(timeout=45000);checks.append('subject_page_entry')
        assert not errors,errors
        browser.close()
    (a.out/'browser-acceptance.json').write_text(json.dumps({'checks':checks,'page_errors':errors,'passed':True},ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'checks':checks,'page_errors':errors}))

if __name__=='__main__':main()
