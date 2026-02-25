import requests
import re
from urllib.parse import urljoin

def extract_pdfs_from_content(content, base_url):
    """Helper to extract PDF links from HTML content using Regex only (for local verification)."""
    pdf_paths = []
    
    print("Attempting regex extraction for PDF links...")
    try:
        html_text = content.decode('utf-8', errors='ignore')
        # Regex to find links ending in .pdf
        # Pattern: <a ... href="path/to/file.pdf" ...>FileName</a>
        pdf_pattern = r'<a[^>]+href=["\']([^"\']+\.pdf)["\'][^>]*>(.*?)</a>'
        matches = re.findall(pdf_pattern, html_text, re.IGNORECASE | re.DOTALL)
        
        for href, text in matches:
            # Clean up text (remove HTML tags if any)
            clean_text = re.sub('<[^<]+?>', '', text).strip()
            absolute_url = urljoin(base_url, href)
            pdf_paths.append((absolute_url, clean_text))
            
        if pdf_paths:
            print(f"Found {len(pdf_paths)} PDFs via Regex")
            # De-duplicate
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

def get_pdf_paths_from_html_fixed(doc_url, proxies=None):
    print(f"Testing URL: {doc_url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive"
    }

    # 1. Try Direct fetch
    try:
        print("Attempting direct fetch...")
        response = requests.get(doc_url, headers=headers, timeout=15)
        if response.status_code == 200:
            print("Direct fetch successful.")
            return extract_pdfs_from_content(response.content, doc_url)
        else:
            print(f"Direct fetch failed: {response.status_code}")
    except Exception as e:
        print(f"Direct fetch error: {e}")

    return []

if __name__ == "__main__":
    target_url = "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/zjzczq_ABS/ABS_fxdfyxqgg/202512/t20251219_854864994.html"
    pdfs = get_pdf_paths_from_html_fixed(target_url)
    print("\nResults:")
    if pdfs:
        for url, title in pdfs:
            print(f"- {title}: {url}")
    else:
        print("No PDFs found.")
