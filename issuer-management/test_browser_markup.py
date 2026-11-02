"""Real Chromium regression: current JavaScript must tolerate an older loaded document."""
from pathlib import Path
import re
from playwright.sync_api import sync_playwright, expect


def main():
    root=Path(__file__).parent/'web'
    html=(root/'index.html').read_text(encoding='utf-8')
    # A pre-prototype tab has no overviewRows. The new script must retrieve the
    # complete new shell before binding listeners, rather than dereference null.
    old='<!doctype html><html><head><link rel="stylesheet" href="style.css"></head><body><main><h1 id="name">正在读取主体资料…</h1></main><script src="app.js"></script></body></html>'
    js=(root/'app.js').read_text(encoding='utf-8')
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=False)
        page=browser.new_page(viewport={'width':1600,'height':1100})
        errors=[]
        page.on('pageerror',lambda e:errors.append(str(e)))
        page.route('**/app.js*',lambda r:r.fulfill(status=200,content_type='text/javascript',body=js))
        page.route('**/issuers/',lambda r:r.fulfill(status=200,content_type='text/html',body=old))
        page.route('**/index.html*',lambda r:r.fulfill(status=200,content_type='text/html',body=html))
        page.goto('https://r760.instmarket.com.au:1443/dealviewer/issuers/#issuer=1&tab=metrics')
        expect(page.locator('#metricRows tr')).to_have_count(10,timeout=45000)
        expect(page.locator('#metricsPanel thead th')).to_have_count(10)
        expect(page.locator('#error')).to_be_hidden()
        assert not errors, errors
        page.screenshot(path=str(Path(__file__).resolve().parents[2]/'tmp/issuer-production/legacy-markup-regression.png'),full_page=True)
        print('legacy HTML + current JS: 10 periods, 8 metric columns, no page errors',flush=True)
        browser.close()


if __name__=='__main__':main()
