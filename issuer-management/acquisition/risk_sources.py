"""Fetch authority lists once per batch, archive them, then match every issuer locally.

No SQL/model credentials enter this browser container. Missing or inaccessible
sources produce explicit checks; never convert an incomplete list to no hits.
"""
import argparse
import hashlib
import json
import re
import time
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlparse
from risk_rules import normalize_name, business_day, window_start
from .state import Store, now

SOURCES={
 'nfra':'https://www.nfra.gov.cn/cn/view/pages/ItemList.html?itemId=931&itemPId=923&itemUrl=zhengwuxinxi%2Fxingzhengchufa.html',
 'pbc':'https://www.pbc.gov.cn/zhengwugongkai/4081330/4081344/4081407/4081705/index.html',
 'safe':'https://www.safe.gov.cn/www/illegal?siteid=safe'}


def save_original(root, content):
    digest=hashlib.sha256(content).hexdigest();directory=Path(root)/'originals';directory.mkdir(exist_ok=True)
    target=directory/(digest+'.html')
    if not target.exists():target.write_bytes(content)
    return digest


def penalty_from_text(text, issuer):
    """Conservative structured-text shortcut. Ambiguous components stay pending."""
    name=issuer['name'];normalized=normalize_name(text)
    facts={'party':name,'party_type':'unknown','amounts_complete':False,'amounts_reliable':False,
           'event_date':None,'evidence':{'source':{'page':1,'quote':text}}}
    names=[name,*issuer.get('confirmed_aliases',[])]
    matched=next((n for n in names if normalize_name(n) in normalized),None)
    if not matched:return None
    # Exact legal recipient, excluding a branch suffix after the corporate name.
    branch=re.search(re.escape(normalize_name(matched))+r'[^。；\n]{0,20}(?:分行|支行|分公司|支公司)',normalized)
    if branch:facts['party_type']='branch';return facts
    pattern=re.escape(normalize_name(matched))+r'(?:给予|处以|予以|警告并处|警告并|警告，并处|警告，处以|警告、)?(?:罚款|罚没)(\d[\d,]*(?:\.\d+)?)\s*(亿元|万元|元)'
    matches=list(re.finditer(pattern,normalized))
    # Do not infer completeness from one item in a multi-line decision.
    if len(matches)==1:
        match=matches[0];field='fine_confiscation_total' if '罚没' in match[0] else 'fine_total'
        facts.update(party_type='legal_entity',amounts_complete=True,amounts_reliable=True,unit=match[2])
        facts[field]=match[1];facts['evidence'][field]={'page':1,'quote':match[0]}
    number=re.search(r'[\w\u4e00-\u9fff]+[〔\[]\d{4}[〕\]][\d、\-]+号',normalized)
    if number:facts['decision_number']=number[0]
    return facts


def penalty_from_row(cells, issuer):
    """PBC published nine-column decision table; party/decision/amount/date have distinct cells."""
    if len(cells)!=9:return penalty_from_text('\n'.join(cells),issuer)
    party=normalize_name(cells[1]);names={normalize_name(issuer['name']),*map(normalize_name,issuer.get('confirmed_aliases',[]))}
    if party not in names:
        facts=penalty_from_text('\n'.join(cells),issuer)
        if facts:
            facts['party']=cells[1]
            facts['party_type']='branch' if any(t in cells[1] for t in ('分行','支行','分公司','支公司')) else 'person'
        return facts
    text='\n'.join(cells);facts=penalty_from_text(text,issuer)
    facts.update(party=issuer['name'],party_type='legal_entity',decision_number=normalize_name(cells[2]))
    fines=re.findall(r'罚款\s*(\d[\d,]*(?:\.\d+)?)\s*(亿元|万元|元)',cells[4])
    if len(fines)==1:
        facts.update(fine_total=fines[0][0],unit=fines[0][1],amounts_complete=True,amounts_reliable=True)
    date_match=re.fullmatch(r'\s*(20\d{2})年(\d{1,2})月(\d{1,2})日\s*',cells[6])
    if date_match:
        from datetime import date
        facts['event_date']=date(*map(int,date_match.groups())).isoformat()
    facts['evidence'].update({k:{'page':1,'quote':cells[n]} for k,n in [('party',1),('decision_number',2),('fine_total',4),('event_date',6)]})
    return facts


def collect_authority(context, source, root, batch_id, max_pages=40):
    page=context.new_page();page.set_default_timeout(30000)
    records=[];visited=set();dates=[];snapshots=[]
    try:
        response=page.goto(SOURCES[source],wait_until='domcontentloaded',timeout=45000)
        if source=='nfra':page.wait_for_function("document.body.innerText.includes('行政处罚')",timeout=30000)
        if response and response.status>=400:raise RuntimeError('http_'+str(response.status))
        if source=='nfra':
            # The wrapper contains local and head-office sections. Only the head-office tab is selected.
            tab=page.get_by_text('总局机关',exact=True)
            if tab.count():tab.first.click();page.wait_for_load_state('networkidle')
        for index in range(max_pages):
            html=page.content();text=page.locator('body').inner_text()
            digest=save_original(root,html.encode('utf-8'));snapshots.append(digest)
            if '404 Not Found' in text or 'Access Denied' in text:raise RuntimeError('source_access_denied_or_404')
            if source=='safe':
                if '本级近一年' not in text or not page.locator('#totalRow').count():raise RuntimeError('safe_list_schema_changed')
                total=int(page.locator('#totalRow').inner_text())
                links=page.locator('.cfxx_jg table a').evaluate_all('es=>es.map(e=>({text:e.innerText,url:e.href}))')
                if total==0:break
            else:
                links=page.locator('a').evaluate_all('es=>es.map(e=>({text:e.innerText.trim(),url:e.href}))')
                links=[x for x in links if ('处罚' in x['text'] if source=='nfra' else '罚决' in x['text']) and ('docId=' in x['url'] if source=='nfra' else bool(re.search(r'/\d{15,}/index.html',x['url'])))
                       and ('总局' in x['text'] or source!='nfra')]
                if not links:raise RuntimeError('authority_list_empty_or_schema_changed')
            current={x['url'] for x in links}
            if current and current<=visited:raise RuntimeError('authority_repeated_page')
            for link in links:
                if link['url'] in visited:continue
                if urlparse(link['url']).hostname!=urlparse(SOURCES[source]).hostname:continue
                visited.add(link['url'])
                detail=context.new_page()
                try:
                    time.sleep(1.5)
                    response=detail.goto(link['url'],wait_until='domcontentloaded',timeout=45000)
                    detail.wait_for_function("document.body.innerText.includes('罚')",timeout=30000)
                    if response and response.status>=400:raise RuntimeError('detail_http_'+str(response.status))
                    body=detail.locator('body').inner_text();raw=detail.content().encode('utf-8')
                    h=save_original(root,raw)
                    # Retain the entire original plus row units. Local matching never fetches a new list per issuer.
                    table_rows=detail.locator('tr').evaluate_all('es=>es.filter(e=>!e.querySelector("tr")).map(e=>e.innerText)')
                    cells=detail.locator('tr').evaluate_all('es=>es.map(e=>Array.from(e.querySelectorAll("td")).map(c=>c.innerText.trim())).filter(x=>x.length===9)') if source=='pbc' else []
                    units=[x for x in table_rows if any(t in x for t in ('罚款','罚没','没收'))] or [body]
                    found=re.search(r'(20\d{2})[-年](\d{1,2})[-月](\d{1,2})',body)
                    published=f'{found[1]}-{int(found[2]):02}-{int(found[3]):02}' if found else None
                    if published:dates.append(published)
                    records.append({'source':source,'url':link['url'],'title':link['text'],'digest':h,'text':body,'units':units,'cells':cells,'published':published})
                finally:detail.close()
            if dates and max(dates[-len(links):] or dates)<window_start().isoformat():break
            next_link=page.get_by_text('下一页',exact=True)
            position=re.search(r'当前页\s*[:：]\s*(\d+)\s*/\s*(\d+)',text)
            if position and position[1]==position[2]:break
            if not next_link.count() or not next_link.first.is_visible() or next_link.first.get_attribute('disabled') is not None:break
            if source=='safe' and int(page.locator('#page').inner_text())>=int(page.locator('#total').inner_text()):break
            if index==max_pages-1:raise RuntimeError('authority_page_limit_incomplete')
            time.sleep(1.5);next_link.first.click();page.wait_for_load_state('networkidle')
        else:raise RuntimeError('authority_page_limit_incomplete')
        end=business_day();start=max(window_start(end),end-timedelta(days=365)) if source=='safe' else min(dates) if dates else None
        return {'source':source,'status':'success','since':str(start) if start else None,'until':end.isoformat(),
                'checked_at':now(),'snapshots':snapshots,'records':records}
    except Exception as error:
        try:snapshots.append(save_original(root,page.content().encode('utf-8')))
        except Exception:pass
        return {'source':source,'status':'failed','error':str(error) if isinstance(error,RuntimeError) else type(error).__name__,
                'checked_at':now(),'since':min(dates) if dates else None,'until':max(dates) if dates else None,
                'snapshots':snapshots,'records':records}
    finally:page.close()


def replay(root, inventory, store):
    """New issuers see every retained list without another list request."""
    for path in sorted((Path(root)/'lists').glob('*/*.json')):
        snapshot=json.loads(path.read_text(encoding='utf-8'))
        for issuer in inventory:
            if issuer['monitoring_status']!='active':continue
            marker=str(path.relative_to(root))+':'+str(issuer['id'])
            if store.db.execute("SELECT 1 FROM events WHERE event='list_replayed' AND detail=?",(marker,)).fetchone():continue
            with store.db:
                for record in snapshot['records']:
                    for part in record.get('cells') or record['units']:
                        unit='\n'.join(part) if isinstance(part,list) else part
                        facts=penalty_from_row(part,issuer) if isinstance(part,list) else penalty_from_text(unit,issuer)
                        if facts is None:continue
                        store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',(now(),'authority_material',json.dumps(
                            dict(record,issuer_id=issuer['id'],issuer_name=issuer['name'],facts=facts,pages={1:unit}),ensure_ascii=False)))
                store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',(now(),'list_replayed',marker))


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--issuers',type=Path,required=True);p.add_argument('--batch-file',type=Path,required=True)
    a=p.parse_args();inventory=json.loads(a.issuers.read_text(encoding='utf-8'));batch=json.loads(a.batch_file.read_text());store=Store(a.root)
    from playwright.sync_api import sync_playwright
    with store.lock(),sync_playwright() as playwright:
        from .network import proxy_config
        proxy=proxy_config()
        browser=playwright.chromium.launch(headless=True,**({'proxy':proxy} if proxy else {}));context=browser.new_context(locale='zh-CN')
        try:
            for source in SOURCES:
                target=a.root/'lists'/batch['id']/(source+'.json');target.parent.mkdir(parents=True,exist_ok=True)
                prior=json.loads(target.read_text(encoding='utf-8')) if target.exists() else None
                if prior and prior['status']=='success':continue
                result=collect_authority(context,source,a.root,batch['id'])
                if prior:
                    history=target.with_name(source+'-'+hashlib.sha256(target.read_bytes()).hexdigest()+'.json')
                    if not history.exists():history.write_bytes(target.read_bytes())
                target.write_text(json.dumps(result,ensure_ascii=False),encoding='utf-8')
                for issuer in inventory:
                    if issuer['monitoring_status']!='active':continue
                    with store.db:store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',(now(),'source_check',json.dumps(
                        {k:v for k,v in result.items() if k not in ('records','snapshots')}|{'issuer_id':issuer['id'],'archive':str(target.relative_to(a.root))},ensure_ascii=False)))
            for source,reason in [('qcc','企查查尚未开通；缺少应用凭据、接口权益及许可/历史/分页/费用试用结果'),('international','未接入Wind许可接口；国际评级仅在获知事件后人工补录')]:
                marker=batch['id']+':'+source
                if store.db.execute("SELECT 1 FROM events WHERE event='source_unconnected' AND detail=?",(marker,)).fetchone():continue
                with store.db:
                    for issuer in inventory:
                        store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',(now(),'source_check',json.dumps({'issuer_id':issuer['id'],'source':source,'status':'not_connected','error':reason,'checked_at':now()},ensure_ascii=False)))
                    store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',(now(),'source_unconnected',marker))
            replay(a.root,inventory,store)
        finally:browser.close();store.db.close()

if __name__=='__main__':main()
