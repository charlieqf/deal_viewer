import argparse
import os
import random
import re
import time
from datetime import datetime
from dateutil.parser import parse
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service


def parse_args():
    parser = argparse.ArgumentParser(
        description="PDF crawl demo: download PDFs within a datetime range."
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start datetime (e.g. '2024-06-01 00:00:00' or '2024-06-01')",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End datetime (e.g. '2024-06-30 23:59:59' or '2024-06-30')",
    )
    parser.add_argument(
        "--output-dir",
        default="demo_downloads",
        help="Local directory to save PDFs",
    )
    parser.add_argument(
        "--proxy-url",
        default=None,
        help="Override proxy URL (e.g. http://user:pass@host:port)",
    )
    parser.add_argument(
        "--no-proxy",
        action="store_true",
        help="Disable proxy usage for HTTP requests",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=30,
        help="Timeout (seconds) for direct list request",
    )
    return parser.parse_args()


# ProxyJet settings
proxy_string = (
    "in.proxy-jet.io:1010:2506034iYZQ-resi_region-CN_Guangdong_Guangzhou-ip-7193938:rUGciFpmX7CwT12"
)
parts = proxy_string.split(":")
hostname = parts[0]
port = parts[1]
username = parts[2]
password = parts[3]

proxy_url = f"http://{username}:{password}@{hostname}:{port}"
DEFAULT_PROXIES = {
    "http": proxy_url,
    "https": proxy_url,
}
ACTIVE_PROXIES = DEFAULT_PROXIES

CHINABOND_LIST_URL = "https://www.chinabond.com.cn/cbiw/trs/getContentByConditions"


def in_range(issue_time, start_dt, end_dt):
    try:
        issue_dt = parse(issue_time)
    except Exception:
        return False

    if start_dt and issue_dt < start_dt:
        return False
    if end_dt and issue_dt > end_dt:
        return False
    return True


def sanitize_filename(name):
    if not name:
        name = "document"
    invalid = '<>:/\\|?*"'
    for ch in invalid:
        name = name.replace(ch, "_")
    name = " ".join(name.split()).strip()
    if len(name) > 180:
        name = name[:180].rstrip()
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name


def get_pdf_paths_from_html(doc_url, proxies):
    print(f"Fetching detail page: {doc_url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
            "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    }

    try:
        print("Attempting direct fetch without proxy...")
        response = requests.get(doc_url, headers=headers, timeout=20)
        if response.status_code == 200:
            print("Direct fetch successful.")
            content = response.content
            return extract_pdfs_from_content(content, doc_url)
        print(f"Direct fetch failed with status: {response.status_code}")
    except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        print(f"Direct fetch encountered error: {e}")

    print("Attempting fetch with proxy...")
    try:
        response = requests.get(doc_url, headers=headers, proxies=proxies, timeout=30)
        if response.status_code == 200:
            print("Proxy fetch successful.")
            content = response.content
            return extract_pdfs_from_content(content, doc_url)
        print(f"Proxy fetch failed with status: {response.status_code}")
    except Exception as e:
        print(f"Error scraping detail page with proxy: {e}")
    return []


def extract_pdfs_from_content(content, base_url):
    pdf_paths = []

    try:
        soup = BeautifulSoup(content, "html.parser")
        file_box = soup.find("div", class_="allDetailFileBox")
        if file_box:
            for link in file_box.find_all("a"):
                href = link.get("href")
                text = link.get_text(strip=True)
                if href and href.lower().endswith(".pdf"):
                    absolute_url = urljoin(base_url, href)
                    pdf_paths.append((absolute_url, text))
            if pdf_paths:
                print(f"Found {len(pdf_paths)} PDFs via BeautifulSoup")
                return pdf_paths
    except Exception as e:
        print(f"BeautifulSoup parsing failed: {e}")

    print("Attempting regex fallback for PDF extraction...")
    try:
        html_text = content.decode("utf-8", errors="ignore")
        pdf_pattern = r'<a[^>]+href=["\']([^"\']+\.pdf)["\'][^>]*>(.*?)</a>'
        matches = re.findall(pdf_pattern, html_text, re.IGNORECASE | re.DOTALL)

        for href, text in matches:
            clean_text = re.sub("<[^<]+?>", "", text).strip()
            absolute_url = urljoin(base_url, href)
            pdf_paths.append((absolute_url, clean_text))

        if pdf_paths:
            print(f"Found {len(pdf_paths)} PDFs via Regex")
            unique_pdfs = []
            seen_urls = set()
            for url, title in pdf_paths:
                if url not in seen_urls:
                    unique_pdfs.append((url, title))
                    seen_urls.add(url)
            return unique_pdfs
    except Exception as e:
        print(f"Regex extraction failed: {e}")

    return []


def get_web_pdf_content_with_retry(web_pdf_path, retries=5):
    for attempt in range(retries):
        try:
            return get_web_pdf_content(web_pdf_path)
        except Exception as e:
            print(
                f"Error occurred while getting PDF content from {web_pdf_path}: {e}. "
                f"Retrying {retries - attempt - 1} more times."
            )
            time.sleep(5)

    raise Exception("Failed to get PDF content after multiple attempts")


def get_web_pdf_content(web_pdf_path):
    encoded_url = quote(web_pdf_path, safe=":/")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/69.0.3497.100 Safari/537.36"
        ),
        "Accept": "application/pdf",
    }

    print("Attempting to download without proxy first...")
    try:
        response = requests.get(encoded_url, headers=headers, timeout=(10, 30))

        if response.status_code == 200 and response.headers.get("Content-Type") == "application/pdf":
            print("Download successful without proxy")
            return True, response.content

        ip_related_failure = False
        if response.status_code in [403, 429, 451]:
            ip_related_failure = True
            print(f"IP-related status code detected: {response.status_code}")
        elif response.status_code != 200:
            ip_block_indicators = [
                "blocked",
                "forbidden",
                "access denied",
                "IP",
                "地址被禁止",
                "访问受限",
                "访问被拒绝",
            ]
            response_text = response.text.lower()
            for indicator in ip_block_indicators:
                if indicator.lower() in response_text:
                    ip_related_failure = True
                    print(f"IP blocking message detected: {indicator}")
                    break

        if not ip_related_failure:
            print(
                f"Failed to download without proxy. Status: {response.status_code}. Not IP related."
            )
            return False, response.text
    except Exception as e:
        print(f"Error during non-proxy download attempt: {e}")
        if not isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
            return False, str(e)

    if not ACTIVE_PROXIES:
        return False, "Proxy disabled"

    print("Trying download with SmartProxy...")
    try:
        response = requests.get(
            encoded_url, headers=headers, proxies=ACTIVE_PROXIES, timeout=(10, 30)
        )

        if response.headers.get("Content-Type") == "application/pdf":
            print("Successfully downloaded with proxy")
            return True, response.content

        print(f"Failed to download with proxy. Status: {response.status_code}")
        return False, response.text
    except Exception as e:
        print(f"Error during proxy download attempt: {e}")
        return False, str(e)


def fetch_list_data(headers, data, proxies, direct_timeout):
    print("Attempting direct request...")
    try:
        direct_response = requests.post(
            CHINABOND_LIST_URL, json=data, headers=headers, timeout=(direct_timeout, 45)
        )
        if direct_response.status_code == 200:
            direct_data = direct_response.json()
            if direct_data.get("success"):
                print("Direct request successful")
                return direct_data
            print(f"Direct request returned non-success: {direct_data}")
        else:
            raise Exception(f"Request failed: {direct_response.status_code}")
    except Exception as e:
        print(f"Direct request failed: {e}, trying proxy...")

    if not proxies:
        raise Exception("Proxy disabled and direct request failed")

    print("Requesting with ProxyJet...")
    proxy_response = requests.post(
        CHINABOND_LIST_URL, json=data, headers=headers, proxies=proxies, timeout=(15, 45)
    )
    if proxy_response.status_code == 200:
        response_data = proxy_response.json()
        print("Proxy request successful")
        return response_data

    raise Exception(f"Proxy request failed: {proxy_response.status_code}")


def build_entries(list_data, start_dt, end_dt, proxies):
    entries = []
    for item in list_data:
        issue_time = item.get("shengXiaoShiJian", "")
        doc_title = (item.get("docTitle", "") or "").strip()
        doc_url = item.get("docPubUrl", "")
        appendix_ids = item.get("appendixIds", "")

        if not issue_time or not doc_url:
            continue
        if not in_range(issue_time, start_dt, end_dt):
            continue

        pdf_path_home = doc_url.rsplit("/", 1)[0]
        pdf_paths = []

        if appendix_ids:
            try:
                pdf_name = appendix_ids.split("=")[1]
                pdf_paths.append((f"{pdf_path_home}/{pdf_name}", doc_title))
            except Exception:
                print(f"Failed to parse appendixIds: {appendix_ids}")
                continue
        else:
            print(f"No appendixIds for {doc_title}, scraping detail page...")
            found_pdfs = get_pdf_paths_from_html(doc_url, proxies)
            for url, text in found_pdfs:
                display_title = text.strip() if text else doc_title
                pdf_paths.append((url, display_title))

        for pdf_url, display_title in pdf_paths:
            entries.append(
                {
                    "title": doc_title,
                    "issue_time": issue_time,
                    "pdf_url": pdf_url,
                    "display_title": display_title or doc_title,
                }
            )

    return entries


def download_pdfs(entries, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    print(f"Downloading {len(entries)} PDFs to {output_dir}...")

    downloaded = 0
    for entry in entries:
        filename = sanitize_filename(entry.get("display_title") or entry.get("title"))
        file_path = os.path.join(output_dir, filename)

        if os.path.exists(file_path):
            print(f"Skip existing: {file_path}")
            continue

        print(f"Downloading {entry['pdf_url']} -> {file_path}")
        result, content = get_web_pdf_content_with_retry(entry["pdf_url"])
        if result and isinstance(content, (bytes, bytearray)):
            with open(file_path, "wb") as f:
                f.write(content)
            downloaded += 1
        else:
            print(f"Download failed: {entry['pdf_url']}")

    print(f"Done. Downloaded {downloaded} PDFs.")


def use_selenium(start_dt, end_dt, output_dir, proxies, direct_timeout):
    print("Connecting to the website...")
    driver.get(
        "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/"
    )
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    time.sleep(3)

    data = {
        "parentChnlName": "zqzl_zjzzczj",
        "excludeChnlNames": [],
        "childChnlDesc": "付息兑付与行权公告",
        "hasAppendix": True,
        "siteName": "chinaBond",
        "pageSize": 100,
        "pageNum": 1,
        "queryParam": {
            "keywords": "",
            "startDate": "",
            "endDate": "",
            "reportType": "",
            "reportYear": "",
            "ratingAgency": "",
        },
    }

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    ]

    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.chinabond.com.cn",
        "Referer": "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/",
        "Connection": "keep-alive",
    }

    response_data = fetch_list_data(headers, data, proxies, direct_timeout)

    if not isinstance(response_data, dict) or not response_data.get("success"):
        raise Exception("Response data invalid or missing success flag")

    list_data = response_data.get("data", {}).get("list", [])
    entries = build_entries(list_data, start_dt, end_dt, proxies)

    print(f"Matched {len(entries)} PDFs within range")
    download_pdfs(entries, output_dir)


if __name__ == "__main__":
    args = parse_args()
    start_dt = parse(args.start)
    end_dt = parse(args.end)
    if args.no_proxy:
        ACTIVE_PROXIES = None
        selected_proxy_url = None
    elif args.proxy_url:
        selected_proxy_url = args.proxy_url
        ACTIVE_PROXIES = {"http": selected_proxy_url, "https": selected_proxy_url}
    else:
        selected_proxy_url = proxy_url
        ACTIVE_PROXIES = DEFAULT_PROXIES

    options = webdriver.ChromeOptions()
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-debugging-port=9222")
    if selected_proxy_url:
        options.add_argument(f"--proxy-server={selected_proxy_url}")
    options.add_argument("--log-level=3")

    service = Service("/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)

    try:
        use_selenium(
            start_dt,
            end_dt,
            args.output_dir,
            ACTIVE_PROXIES,
            args.request_timeout,
        )
    finally:
        driver.close()
        driver.quit()
