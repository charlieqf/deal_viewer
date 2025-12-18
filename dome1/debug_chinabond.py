import requests
import json
import re
from urllib.parse import urljoin

# Proxy setup
proxy_string = "proxy-jet.io:1010:2506034iYZQ-resi_region-AU_Newsouthwales_Parramatta:rUGciFpmX7CwT12"
parts = proxy_string.split(":")
hostname = parts[0]
port = parts[1]
username = parts[2]
password = parts[3]
proxy_url = f"http://{username}:{password}@{hostname}:{port}"

proxies = {
    "http": proxy_url,
    "https": proxy_url,
}

def get_pdf_paths_from_html_regex(doc_url, proxies):
    print(f"Fetching detail page: {doc_url}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/69.0.3497.100",
        }
        response = requests.get(doc_url, headers=headers, proxies=proxies, timeout=30)
        print(f"Detail page status: {response.status_code}")
        
        content = response.text
        # Look for div class="allDetailFileBox" matches specifically or just all .pdf links if simpler
        # For simplicity in debug, let's find all hrefs ending in .pdf
        
        pdf_pattern = r'<a[^>]+href="([^"]+\.pdf)"[^>]*>([^<]+)</a>'
        matches = re.findall(pdf_pattern, content, re.IGNORECASE)
        
        pdf_paths = []
        for href, text in matches:
            text = text.strip()
            absolute_url = urljoin(doc_url, href)
            pdf_paths.append((absolute_url, text))
            print(f"Found PDF: {text}")

        # Also try single quotes
        pdf_pattern_sq = r"<a[^>]+href='([^']+\.pdf)'[^>]*>([^<]+)</a>"
        matches_sq = re.findall(pdf_pattern_sq, content, re.IGNORECASE)
        for href, text in matches_sq:
            text = text.strip()
            absolute_url = urljoin(doc_url, href)
            pdf_paths.append((absolute_url, text))
            print(f"Found PDF (sq): {text}")
                
        return pdf_paths

    except Exception as e:
        print(f"Error scraping detail page: {e}")
        return []

def debug_product():
    url = "https://www.chinabond.com.cn/cbiw/trs/getContentByConditions"
    
    # Matching the payload from the original script
    data = {
        "parentChnlName": "zqzl_zjzzczj",
        "excludeParentChnlNames": [],
        "childChnlDesc": "发行文件",
        "hasAppendix": True,
        "siteName": "chinaBond",
        "pageSize": 50, # Increased strictly for search
        "pageNum": 1,
        "queryParam": {
            "keywords": "", # Search specific product
            "startDate": "",
            "endDate": "",
            "reportType": "",
            "reportYear": "",
            "ratingAgency": ""
        }
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/"
    }

    print("Querying API...")
    try:
        response = requests.post(url, json=data, headers=headers, proxies=proxies, timeout=30)
        print(f"API Status: {response.status_code}")
        
        if response.status_code == 200:
            res_json = response.json()
            if res_json.get("success"):
                items = res_json["data"]["list"]
                target_name = "浦鑫归航2025年第九期不良资产支持证券发行文件"
                
                found_item = None
                for item in items:
                    if target_name in item["docTitle"]:
                        found_item = item
                        break
                
                if found_item:
                    print("Found Product Item:")
                    print(json.dumps(found_item, indent=2, ensure_ascii=False))
                    
                    appendix_ids = found_item.get("appendixIds")
                    doc_url = found_item.get("docPubUrl")
                    
                    if appendix_ids:
                        print(f"Has appendixIds: {appendix_ids}")
                    else:
                        print("appendixIds is null/empty. Attempting scrape...")
                        if doc_url:
                            pdfs = get_pdf_paths_from_html_regex(doc_url, proxies)
                            print(f"Scraped PDFs: {pdfs}")
                        else:
                            print("No docPubUrl to scrape.")
                else:
                    print(f"Product '{target_name}' not found in first 50 results.")
            else:
                print("API returned success=False")
        else:
            print("Response not 200")
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    debug_product()
