"""Small real-source verification using a specified public search result ID."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright
from .chinamoney import ChinaMoney, attachments
from .state import Store


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--issuer-id',type=int,required=True)
    p.add_argument('--query',required=True);p.add_argument('--source-id',required=True);p.add_argument('--kind',choices=['financial','rating'],required=True)
    a=p.parse_args();store=Store(a.root)
    try:
        with store.lock(),sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                adapter=ChinaMoney(browser.new_context(locale='zh-CN',accept_downloads=True),max_pages=3)
                for result in adapter.search(a.query):
                    for item in result['pageItems']:
                        if str(item['id'])!=a.source_id:continue
                        for attachment in attachments(item):
                            if attachment['published'] and attachment['published']<'2024-01-01':
                                raise ValueError('canary_publication_before_2024')
                            content=adapter.download(attachment)
                            first=store.archive(a.issuer_id,a.kind,attachment,content)
                            replay=store.archive(a.issuer_id,a.kind,attachment,content)
                            assert replay==(first[0],False)
                            print(json.dumps({'source_id':a.source_id,'bytes':len(content),'journal_id':first[0],'idempotent':True}))
                            return
                raise RuntimeError('canary_source_not_found')
            finally:browser.close()
    finally:store.db.close()


if __name__=='__main__':main()
