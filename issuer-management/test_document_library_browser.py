"""Read-only acceptance of the public document library and original PDF download."""
import argparse
import json
from pathlib import Path
import requests
from playwright.sync_api import sync_playwright, expect

def main():
    p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--out',type=Path,required=True)
    args=p.parse_args();base=args.base.rstrip('/')+'/';args.out.mkdir(parents=True,exist_ok=True)
    def library(**kw):
        r=requests.get(base+'api.ashx',params={'action':'library',**kw},timeout=60);r.raise_for_status();return r.json()
    data=library();assert data['total']>0 and len(data['documents'])<=30
    assert all('StorageKey' not in d and 'Sha256' not in d for d in data['documents'])
    selected=data['documents'][0];issuer=selected['IssuerId']
    filtered=library(issuer=issuer);assert all(d['IssuerId']==issuer for d in filtered['documents'])
    rating=library(type='rating');financial=library(type='financial')
    assert rating['total']+financial['total']==data['total']
    assert all(d['DocumentType']=='issuer_rating' for d in rating['documents'])
    assert all(d['DocumentType']!='issuer_rating' for d in financial['documents'])
    assert library(q=selected['Title'])['total']>=1
    assert library(q='not-existing-document-a018ff')['total']==0
    assert library(q="' OR 1=1 --")['total']==0
    for key,value in [('page','0'),('issuer','../../private'),('year','x'),('type','bad')]:
        assert requests.get(base+'api.ashx',params={'action':'library',key:value},timeout=30).status_code==400
    if data['total']>30:
        second=library(page=2)
        assert not ({d['DocumentId'] for d in data['documents']}&{d['DocumentId'] for d in second['documents']})
    samples=[data['documents'][0]]+[group['documents'][0] for group in (rating,financial) if group['documents']]
    for d in samples:
        url=base+'api.ashx?action=document&download=1&id='+d['DocumentId']
        r=requests.get(url,headers={'Range':'bytes=0-31'},timeout=60)
        assert r.status_code==206 and r.content.startswith(b'%PDF-')
        assert r.headers['Content-Disposition'].startswith('attachment;') and "filename*=UTF-8''" in r.headers['Content-Disposition']
        assert int(r.headers['Content-Range'].split('/')[-1])==d['ByteSize']
    errors=[]
    expect.set_options(timeout=60000)
    with sync_playwright() as pw:
        browser=pw.chromium.launch(channel='chrome',headless=True)
        page=browser.new_context(accept_downloads=True).new_page()
        page.set_viewport_size({'width':1440,'height':1000});page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(base+'documents.html');expect(page.locator('#summary')).to_contain_text(str(data['total']))
        expect(page.locator('#rows tr[data-document-id]')).to_have_count(len(data['documents']))
        page.screenshot(path=str(args.out/'library-desktop.png'))
        page.locator('.table-wrap').evaluate('(el)=>el.scrollTop=500')
        assert abs(page.locator('th').first.bounding_box()['y']-page.locator('.table-wrap').bounding_box()['y'])<3
        page.locator('#issuer').select_option(issuer);expect(page.locator('#summary')).to_contain_text(str(filtered['total']))
        page.locator('#reset').click();expect(page.locator('#summary')).to_contain_text(str(data['total']))
        page.locator('#type').select_option('rating');expect(page.locator('#summary')).to_contain_text(str(rating['total']))
        page.screenshot(path=str(args.out/'library-ratings.png'))
        if rating['documents']:
            with page.expect_download() as event:page.locator('.download').first.click()
            download=event.value;assert download.failure() is None
            path=args.out/'download.pdf';download.save_as(path);assert path.read_bytes().startswith(b'%PDF-')
        page.locator('#reset').click();expect(page.locator('#summary')).to_contain_text(str(data['total']))
        page.locator('#search').fill('not-existing-document-a018ff');page.locator('button[type=submit]').click()
        expect(page.locator('.empty')).to_be_visible()
        page.goto(base+'documents.html?issuer='+issuer);expect(page.locator('#issuer')).to_have_value(issuer)
        expect(page.locator('#summary')).to_contain_text(str(filtered['total']))
        page.set_viewport_size({'width':390,'height':844})
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
        page.screenshot(path=str(args.out/'library-mobile.png'))
        page.goto(base);expect(page.locator('#overview a[href="documents.html"]')).to_be_visible()
        try:expect(page.locator('#overviewRows .product-count')).to_have_count(len(data['issuers']))
        except Exception:
            page.screenshot(path=str(args.out/'overview-failure.png'))
            print({'url':page.url,'error':page.locator('#error').text_content(),'search':page.locator('#search').input_value(),'page_errors':errors},flush=True)
            raise
        page.goto(base+'documents.html');page.set_viewport_size({'width':1440,'height':1000})
        expect(page.locator('#summary')).to_contain_text(str(data['total']))
        browser.close()
    assert not errors,errors
    result={'total_documents':data['total'],'issuers':len(data['issuers']),'rating_documents':rating['total'],'financial_documents':financial['total'],'page_errors':errors,'checks':['filters','pagination','query_validation','no_storage_paths','pdf_range_and_attachment','actual_browser_download','sticky_headers','mobile','shareable_filter_url','overview_regression']}
    (args.out/'acceptance.json').write_text(json.dumps(result,indent=2),encoding='utf-8');print(json.dumps(result))

if __name__=='__main__':main()
