"""Archive the AMAC institutional sanctions list once, match monitored legal entities.

This is a bounded official channel, not a declaration of complete market coverage.
The existing PDF/OCR pipeline decides whether a matched sanction concerns ABS filing.
"""
import argparse
import hashlib
import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from risk_rules import business_day, window_start, normalize_name
from .state import Store, now

BASE = 'https://www.amac.org.cn/zlgl/jlcf/scfjg/'


def parse_list(html, url):
    soup = BeautifulSoup(html, 'html.parser')
    result = []
    for li in soup.select('li'):
        anchor, stamp = li.find('a', href=True), li.find('i')
        if not anchor or not stamp: continue
        day = stamp.get_text(strip=True)
        target = urljoin(url, anchor['href'])
        if not re.fullmatch(r'20\d{2}-\d{2}-\d{2}', day): continue
        if urlparse(target).netloc != 'www.amac.org.cn' or '/zlgl/jlcf/scfjg/' not in target: continue
        result.append({'title': anchor.get_text(strip=True), 'url': target, 'published': day})
    count = re.search(r'createPageHTML\(\s*(\d+)\s*,\s*\d+\s*,\s*[\'"]index', html)
    if not result or not count: raise ValueError('amac_list_schema_changed')
    return result, int(count[1])


def matches(title, issuer):
    # Full legal names/confirmed aliases, not fuzzy substrings or a bank's branch.
    names = [issuer['name'], *issuer.get('confirmed_aliases', [])]
    text = normalize_name(title)
    for name in names:
        value = normalize_name(name)
        if value and re.search(re.escape(value)+r'(?=[)〕】]|$)', text): return True
    return False


def collect(context, root, inventory, batch_id, max_pages=80, pause=time.sleep):
    root = Path(root); store = Store(root)
    start, end = window_start(), business_day()
    directory = root/'amac-lists'/batch_id; directory.mkdir(parents=True, exist_ok=True)
    snapshots, records, seen = [], [], set()
    status, error = 'success', None
    try:
        with store.lock():
            for page in range(max_pages):
                url = BASE + ('index.html' if page == 0 else f'index_{page}.html')
                response = context.request.get(url, timeout=45000)
                if response.status != 200: raise ValueError('amac_http_'+str(response.status))
                raw = response.body(); digest = hashlib.sha256(raw).hexdigest()
                (directory/(digest+'.html')).write_bytes(raw); snapshots.append(digest)
                items, total = parse_list(raw.decode('utf-8'), url)
                keys = {r['url'] for r in items}
                if keys <= seen: raise ValueError('amac_repeated_page')
                seen |= keys
                for item in items:
                    if not start.isoformat() <= item['published'] <= end.isoformat(): continue
                    records.append(item)
                    for issuer in inventory:
                        if issuer['monitoring_status'] != 'active' or not matches(item['title'], issuer): continue
                        known = store.db.execute('SELECT id FROM documents WHERE issuer_id=? AND kind=? AND url=?',
                            (issuer['id'], 'abs_suspension', item['url'])).fetchone()
                        if known: continue
                        targets = [item['url']]
                        if not urlparse(item['url']).path.lower().endswith('.pdf'):
                            detail = context.request.get(item['url'], timeout=45000)
                            if detail.status != 200: raise ValueError('amac_detail_unavailable')
                            content = detail.body(); key = hashlib.sha256(content).hexdigest()
                            (directory/(key+'.html')).write_bytes(content)
                            targets = [urljoin(item['url'], a['href']) for a in BeautifulSoup(content,'html.parser').select('a[href]')
                                       if urlparse(urljoin(item['url'],a['href'])).path.lower().endswith('.pdf')]
                            targets = [u for u in targets if urlparse(u).netloc == 'www.amac.org.cn']
                            if not targets: raise ValueError('amac_matched_original_unavailable')
                        for target in dict.fromkeys(targets):
                            pause(10)
                            pdf = context.request.get(target, timeout=60000)
                            if pdf.status != 200: raise ValueError('amac_pdf_unavailable')
                            attachment = dict(item, url=target, source_id=hashlib.sha256(target.encode()).hexdigest())
                            store.archive(issuer['id'], 'abs_suspension', attachment, pdf.body())
                if min(r['published'] for r in items) < start.isoformat() or page+1 >= total: break
                pause(1.5)
            else: raise ValueError('amac_page_limit_incomplete')
    except Exception as exc:
        status, error = 'failed', str(exc) if isinstance(exc, ValueError) else type(exc).__name__
    result = {'source':'amac_abs_suspension','kind':'abs_suspension','checked_at':now(),
              'status':status,'error':error,'since':start.isoformat(),'until':end.isoformat(),
              'archive':str((directory/'index.json').relative_to(root)), 'records':records, 'snapshots':snapshots}
    (directory/'index.json').write_text(json.dumps(result,ensure_ascii=False),encoding='utf-8')
    with store.db:
        for issuer in inventory:
            if issuer['monitoring_status']=='active':
                check={k:v for k,v in result.items() if k not in ('records','snapshots')}
                store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',
                    (now(),'source_check',json.dumps(check|{'issuer_id':issuer['id']},ensure_ascii=False)))
    store.db.close()
    return result


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True)
    p.add_argument('--issuers',type=Path,required=True);p.add_argument('--batch-file',type=Path,required=True)
    a=p.parse_args()
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        request=pw.request.new_context()
        class Context: pass
        ctx=Context();ctx.request=request
        try: result=collect(ctx,a.root,json.loads(a.issuers.read_text()),json.loads(a.batch_file.read_text())['id'])
        finally: request.dispose()
    print(json.dumps({k:v for k,v in result.items() if k not in ('records','snapshots')}))
    raise SystemExit(0 if result['status']=='success' else 1)


if __name__=='__main__': main()
