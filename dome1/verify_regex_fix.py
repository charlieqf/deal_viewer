
import re
from urllib.parse import urljoin

def test_regex():
    file_path = r"c:\work\code\deal_viewer\deal_viewer\dome1\fxwj_html_response.txt"
    doc_url = "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/zjzczq_ABS/ABS_fxwj_ath/202512/t20251216_854860929.html"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    print(f"Read {len(content)} bytes from file.")

    # Regex for double quotes
    pdf_pattern = r'<a[^>]+href="([^"]+\.pdf)"[^>]*>([^<]+)</a>'
    matches = re.findall(pdf_pattern, content, re.IGNORECASE)
    
    # Regex for single quotes
    pdf_pattern_sq = r"<a[^>]+href='([^']+\.pdf)'[^>]*>([^<]+)</a>"
    matches_sq = re.findall(pdf_pattern_sq, content, re.IGNORECASE)
    
    all_matches = matches + matches_sq
    
    print(f"Found {len(all_matches)} matches.")
    
    pdf_paths = []
    for href, text in all_matches:
        text = text.strip()
        absolute_url = urljoin(doc_url, href)
        if (absolute_url, text) not in pdf_paths:
            pdf_paths.append((absolute_url, text))
            print(f"Found PDF via regex: {text}")

if __name__ == "__main__":
    test_regex()
