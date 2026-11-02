"""Use the public search UI, including its own session/signing mechanism."""
import html
import re
from datetime import datetime
from urllib.parse import parse_qs, urljoin, urlparse, unquote
from zoneinfo import ZoneInfo

BASE = 'https://www.chinamoney.com.cn'
SEARCH = BASE + '/chinese/qwjsn/'


def download_api_url(attachment):
    source_id=str(attachment['source_id'])
    priority=str(attachment.get('priority',0))
    if not source_id.isascii() or not source_id.isdigit() or not priority.isascii() or not priority.isdigit():
        raise ValueError('invalid_attachment_identity')
    return BASE+'/dqs/cm-s-notice-query/fileDownLoad.do?mode=open&contentId='+source_id+'&priority='+priority


def download_public_pdf(attachment,max_bytes):
    # Same public endpoint used by the established ABN crawler. Use a clean HTTP
    # session; browser-generated signed download requests can return 403 here.
    import requests
    from .network import proxy_config,requests_proxies
    url=download_api_url(attachment)
    with requests.Session() as session:
        session.trust_env=False
        with session.get(url,timeout=(10,45),stream=True,allow_redirects=False,proxies=requests_proxies(proxy_config())) as response:
            check_download_status(response.status_code)
            if int(response.headers.get('content-length','0'))>max_bytes:
                raise ValueError('pdf_too_large')
            chunks=[];size=0
            for chunk in response.iter_content(65536):
                size+=len(chunk)
                if size>max_bytes:raise ValueError('pdf_too_large')
                chunks.append(chunk)
            content=b''.join(chunks)
            if not content.startswith(b'%PDF-'):raise ValueError('download_not_pdf')
            return content


class DownloadHTTPError(RuntimeError):
    def __init__(self,status):
        self.status=status
        super().__init__('download_http_'+str(status))


def check_download_status(status):
    if not 200<=status<300:raise DownloadHTTPError(status)


def older_page(items,since):
    if not since or not items or any(not x.get('releaseDate') for x in items):return False
    try:dates=[datetime.fromtimestamp(int(x['releaseDate'])/1000,ZoneInfo('Asia/Shanghai')).date().isoformat() for x in items]
    except (ValueError,TypeError,OverflowError):return False
    return dates==sorted(dates,reverse=True) and max(dates)<since


def allowed_url(value):
    url = urljoin(BASE, value)
    parsed = urlparse(url)
    if (parsed.scheme != 'https' or parsed.hostname != 'www.chinamoney.com.cn'
            or parsed.username or parsed.password or parsed.port not in (None, 443)):
        raise ValueError('source_url_not_allowed')
    return url


def result_response(response, expected_page):
    parsed = urlparse(response.url)
    return (parsed.hostname == 'www.chinamoney.com.cn'
            and parsed.path == '/ses/rest/cm-u-notice-ses-cn/query'
            and parse_qs(parsed.query).get('pageIndex') == [str(expected_page)])


def parse_result(body, expected_page):
    if str(body.get('head', {}).get('rep_code')) != '200':
        raise ValueError('search_response_error')
    result = body.get('data', {}).get('result')
    if not isinstance(result, dict) or int(result.get('pageIndex', -1)) != expected_page:
        raise ValueError('search_schema_or_page_changed')
    if not isinstance(result.get('pageItems'), list) or int(result.get('total', -1)) < 0:
        raise ValueError('search_schema_changed')
    return result


def attachments(item):
    title = html.unescape(re.sub('<[^>]+>', '', item['title']))
    published = None
    if item.get('releaseDate'):
        published = datetime.fromtimestamp(item['releaseDate'] / 1000, ZoneInfo('Asia/Shanghai')).date().isoformat()
    for priority, path in enumerate(item.get('paths', [])):
        url = allowed_url(path)
        if urlparse(url).path.lower().endswith('.pdf'):
            yield dict(source_id=item['id'], title=title, url=url, published=published,
                       filename=unquote(urlparse(url).path.rsplit('/',1)[-1]),
                       priority=priority, detail_url=allowed_url(item['dealPath']) if item.get('dealPath') else None)


class ChinaMoney:
    def __init__(self, context, max_pages=100, delay_ms=1500, since=None):
        self.context = context
        self.max_pages = max_pages
        self.delay_ms = delay_ms
        self.since = since
        self.last_download = 0
        self.page = context.new_page()
        self.page.set_default_timeout(45000)

    def search(self, query):
        from .site_lock import site_slot
        self.page.goto(SEARCH, wait_until='domcontentloaded')
        self.page.locator('#searchText').fill(query)
        self.page.locator('#containTitle').check()
        self.page.locator('#containTitle1').uncheck()
        if self.since and getattr(self,'until',None):
            self.page.locator('#pssf-ch-2-6').check()
            # The site's jQuery date inputs are readonly. Use their datepicker
            # API so its internal selection and the submitted values agree.
            for selector,value in [('#psawf-start-date',self.since),('#psawf-end-date',self.until)]:
                field=self.page.locator(selector)
                field.evaluate('(el,value) => window.jQuery(el).datepicker("setDate",value)',value)
                if field.input_value()!=value:raise ValueError('search_date_not_applied')
        with site_slot('search'), self.page.expect_response(lambda r: result_response(r, 1)) as response:
            self.page.get_by_text('查询', exact=True).click()
        current = response.value
        seen_ids = set()
        for index in range(1, self.max_pages + 1):
            if current.status != 200:
                raise RuntimeError('search_http_' + str(current.status))
            result = parse_result(current.json(), index)
            items = result['pageItems']
            ids = {str(x['id']) for x in items}
            if items and ids <= seen_ids:
                raise RuntimeError('search_repeated_page')
            seen_ids.update(ids)
            stop_at_date=parse_qs(urlparse(current.url).query).get('sort')==['date'] and older_page(items,self.since)
            if stop_at_date:result['stopped_before_since']=self.since
            yield result
            if stop_at_date:return
            if len(seen_ids) >= int(result['total']):
                return
            if not items:
                raise RuntimeError('search_incomplete_empty_page')
            if index == self.max_pages:
                raise RuntimeError('search_page_limit_incomplete')
            self.page.wait_for_timeout(self.delay_ms)
            with site_slot('search'), self.page.expect_response(lambda r: result_response(r, index + 1)) as response:
                self.page.get_by_text('下一页', exact=True).click()
            current = response.value

    def download(self, attachment, max_bytes=100 * 1024 * 1024):
        from .site_lock import site_slot
        with site_slot('download'):
            return self._download(attachment,max_bytes)

    def _download(self, attachment, max_bytes):
        import time
        wait=10-(time.monotonic()-self.last_download)
        if wait>0:time.sleep(wait)
        self.last_download=time.monotonic()
        url = allowed_url(attachment['url'])
        download_api_url(attachment)  # Reject invalid identity before either transport.
        import requests
        try:
            return download_public_pdf(attachment,max_bytes)
        except DownloadHTTPError as error:
            if error.status in (403,407,429):raise
        except requests.RequestException:
            pass  # The website's browser download remains the fallback.
        except ValueError as error:
            if str(error)!='download_not_pdf':raise
        if attachment.get('detail_url'):
            # The public detail page signs its own download request. Never persist that URL/token.
            page = self.context.new_page()
            try:
                page.goto(allowed_url(attachment['detail_url']), wait_until='domcontentloaded', timeout=45000)
                priority = int(attachment['priority'])
                links = page.locator('a[onclick*="makeFDLT"]')
                links.first.wait_for()
                matches = [link for link in links.all() if re.search(
                    r'(?:[?&])priority='+str(priority)+r'''(?:&|['"])''',link.get_attribute('onclick') or '')]
                if len(matches)!=1:raise ValueError('download_link_mapping_changed')
                link=matches[0]
                with page.expect_download(timeout=90000) as pending:
                    with page.expect_response(lambda r:urlparse(r.url).path.endswith('/fileDownLoad.do'),timeout=45000) as network:
                        link.click()
                    check_download_status(network.value.status)
                download = pending.value
                allowed_url(download.url)
                if download.failure(): raise RuntimeError('browser_download_failed')
                path = download.path()
                if path.stat().st_size > max_bytes: raise ValueError('pdf_too_large')
                return path.read_bytes()
            finally:
                page.close()
        response = self.context.request.get(url, timeout=90000, max_redirects=0)
        try:
            check_download_status(response.status)
            if int(response.headers.get('content-length', '0')) > max_bytes:
                raise ValueError('pdf_too_large')
            body = response.body()
            if len(body) > max_bytes:
                raise ValueError('pdf_too_large')
            return body
        finally:
            response.dispose()
