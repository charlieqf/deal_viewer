"""Monthly acquisition CLI. Downloading is explicitly distinct from SQL ingestion."""
import argparse
import json
import time
from pathlib import Path

from .chinamoney import ChinaMoney, attachments, DownloadHTTPError
from .state import Store, now
from risk_rules import default_title_selected, DEFAULT_TERMS

KEYWORDS = {
    'financial': ['年度报告', '半年度报告', '季度报告', '财务', '信息披露公告'],
    'rating': ['主体 评级', '评级'],
    'default': list(DEFAULT_TERMS),
}


def collect(store, adapter, issuers, since='2024-01-01', kinds=('financial', 'rating'), resume=False,
            boundary=None, intervals=None):
    boundary=boundary or now()[:7]+'-01T00:00:00+00:00'
    completed=set()
    touched=set()
    if resume:
        for row in store.db.execute('SELECT s.* FROM searches s JOIN runs r ON r.id=s.run_id WHERE r.started>=?',(boundary,)):
            touched.add(row['issuer_id'])
            if row['status']=='complete':completed.add((row['issuer_id'],row['kind'],row['query']))
        issuers=sorted(issuers,key=lambda x:x['id'] in touched)
    run = store.start()
    consecutive_blocked=0
    summary = dict(run_id=run, downloaded=0, reused=0, failed=0, searches=0, skipped_paused=0)
    try:
        for issuer in issuers:
            if issuer['monitoring_status'] != 'active':
                summary['skipped_paused'] += 1
                continue
            # Titles sometimes omit the corporate suffix. This is a search variant only;
            # identity validation still uses the legal name and confirmed aliases.
            search_name=issuer['name']
            for suffix in ('股份有限公司','有限责任公司','有限公司'):
                if search_name.endswith(suffix):
                    search_name=search_name[:-len(suffix)];break
            names = list(dict.fromkeys([issuer['name'], search_name, *issuer.get('confirmed_aliases', [])]))
            for kind in kinds:
                interval=(intervals or {}).get(str(issuer['id'])+':'+kind,{'since':since,'until':None})
                since=interval['since']
                adapter.since=since
                adapter.until=interval['until']
                downloaded_urls = {r[0] for r in store.db.execute(
                    'SELECT url FROM documents WHERE issuer_id=? AND kind=? AND last_seen>=?',
                    (issuer['id'],kind,boundary))} if resume else set()
                failed_urls=set()
                for name in names:
                    for keyword in KEYWORDS[kind]:
                        query = name + ' ' + keyword
                        if (issuer['id'],kind,query) in completed:continue
                        seen = total = 0
                        summary['searches'] += 1
                        store.search(run, issuer['id'], kind, query, 'running')
                        for attempt in range(2):
                            seen = 0
                            download_errors = 0
                            try:
                                for result in adapter.search(query):
                                    if intervals:
                                        import hashlib
                                        raw=json.dumps(result,ensure_ascii=False,sort_keys=True).encode('utf-8')
                                        digest=hashlib.sha256(raw).hexdigest()
                                        directory=store.root/'search-responses';directory.mkdir(exist_ok=True)
                                        target=directory/(digest+'.json')
                                        if not target.exists():target.write_bytes(raw)
                                        with store.db:store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',
                                            (now(),'search_page',json.dumps({'run_id':run,'issuer_id':issuer['id'],'kind':kind,'query':query,'page':result.get('pageIndex'),'archive':'search-responses/'+target.name},ensure_ascii=False)))
                                    total = result['total']; seen += len(result['pageItems'])
                                    for item in result['pageItems']:
                                        for attachment in attachments(item):
                                            if kind=='default' and not default_title_selected(attachment['title'],attachment.get('filename','')):
                                                with store.db:store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',
                                                    (now(),'title_filtered',json.dumps({'issuer_id':issuer['id'],'title':attachment['title'],'url':attachment['url'],'reason':'标题筛选未下载'},ensure_ascii=False)))
                                                continue
                                            # Unknown publication date remains eligible for body validation.
                                            if attachment['published'] and attachment['published'] < since:
                                                continue
                                            if interval['until'] and attachment['published'] and attachment['published']>interval['until']:
                                                continue
                                            if kind=='financial' and any(term in attachment['title'] for term in ('募集资金使用','存续期评估认证')):
                                                continue
                                            if attachment['url'] in downloaded_urls:
                                                continue
                                            if attachment['url'] in failed_urls:
                                                download_errors+=1
                                                continue
                                            try:
                                                content = adapter.download(attachment)
                                                _, created = store.archive(issuer['id'], kind, attachment, content)
                                            except Exception as error:
                                                download_errors += 1
                                                failed_urls.add(attachment['url'])
                                                with store.db:
                                                    store.db.execute('INSERT OR REPLACE INTO download_failures VALUES(?,?,?,?,?)',
                                                        (issuer['id'],kind,attachment['url'],str(error) if isinstance(error,DownloadHTTPError) else type(error).__name__,now()))
                                                    store.db.execute('INSERT OR REPLACE INTO download_failure_details VALUES(?,?,?,?,?,?)',
                                                        (issuer['id'],kind,attachment['url'],str(attachment['source_id']),attachment['title'],attachment['published']))
                                                consecutive_blocked=consecutive_blocked+1 if isinstance(error,DownloadHTTPError) and error.status in (403,407,429) else 0
                                                if consecutive_blocked>=5:
                                                    summary['failed']+=1
                                                    summary['access_blocked']=True
                                                    store.search(run,issuer['id'],kind,query,'failed',total,seen,'source_access_blocked')
                                                    store.finish(run,'partial',summary)
                                                    return summary
                                                continue
                                            consecutive_blocked=0
                                            with store.db:
                                                store.db.execute('DELETE FROM download_failures WHERE issuer_id=? AND kind=? AND url=?',
                                                    (issuer['id'],kind,attachment['url']))
                                            downloaded_urls.add(attachment['url'])
                                            summary['downloaded' if created else 'reused'] += 1
                                    store.search(run, issuer['id'], kind, query, 'running', total, seen)
                                    if result.get('stopped_before_since'):
                                        with store.db:store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',
                                            (now(),'search_date_boundary',json.dumps({'run_id':run,'issuer_id':issuer['id'],'kind':kind,'query':query,'since':since},ensure_ascii=False)))
                                if download_errors:
                                    store.search(run, issuer['id'], kind, query, 'failed',total,seen,'attachments_unavailable')
                                    summary['failed']+=1
                                    break
                                store.search(run, issuer['id'], kind, query, 'complete', total, seen)
                                break
                            except Exception as error:
                                # Do not log exception strings: browser errors may contain signed URLs.
                                store.search(run, issuer['id'], kind, query, 'failed', total, seen, type(error).__name__)
                                if attempt == 1: summary['failed'] += 1
                                elif isinstance(error, TimeoutError): time.sleep(2)
        store.finish(run, 'partial' if summary['failed'] else 'complete', summary)
    except BaseException:
        store.finish(run, 'interrupted', summary)
        raise
    finally:
        if intervals:
            from .risk_batch import record_checks
            record_checks(store,run,intervals,boundary)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--issuers', type=Path, required=True, help='Explicit active/paused inventory with confirmed aliases')
    parser.add_argument('--since', default='2024-01-01')
    parser.add_argument('--kind', choices=['financial', 'rating', 'default', 'risk', 'all'], default='all')
    parser.add_argument('--batch-file',type=Path)
    parser.add_argument('--max-pages', type=int, default=100)
    parser.add_argument('--headed', action='store_true')
    parser.add_argument('--resume', action='store_true',help='Resume this month; prioritize issuers not searched yet')
    args = parser.parse_args()
    from datetime import date
    date.fromisoformat(args.since)
    from playwright.sync_api import sync_playwright
    from .network import proxy_config
    store = Store(args.root)
    with store.lock(), sync_playwright() as playwright:
        proxy=proxy_config()
        browser = playwright.chromium.launch(headless=not args.headed,**({'proxy':proxy} if proxy else {}))
        try:
            context = browser.new_context(locale='zh-CN', timezone_id='Asia/Shanghai', accept_downloads=True)
            batch=json.loads(args.batch_file.read_text()) if args.batch_file else {}
            kinds=('rating','default') if args.kind=='risk' else ('financial','rating') if args.kind=='all' else (args.kind,)
            summary = collect(store, ChinaMoney(context, args.max_pages,since=args.since),
                              json.loads(args.issuers.read_text(encoding='utf-8')),
                              args.since,kinds,args.resume,batch.get('started'),batch.get('intervals'))
            print(json.dumps(summary))
        finally:
            browser.close()
    raise SystemExit(1 if summary['failed'] else 0)


if __name__ == '__main__':
    main()
