import requests
import time

def reproduction():
    content_id = "3234240"
    url = f"https://www.chinamoney.com.cn/dqs/cm-s-notice-query/fileDownLoad.do?mode=open&contentId={content_id}&priority=0"
    
    print(f"Target URL: {url}")
    
    # 1. Try without session (like current crawl_pdf)
    print("\n--- Attempt 1: Direct Request (No Session) ---")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        "Accept": "application/pdf"
    }
    try:
        r = requests.get(url, headers=headers, verify=False, timeout=10)
        print(f"Status: {r.status_code}")
        print(f"Content-Type: {r.headers.get('Content-Type')}")
        if r.status_code != 200:
            print(f"Response (first 200 chars): {r.text[:200]}")
    except Exception as e:
        print(f"Error: {e}")

    # 2. Try with Session and Cookies (mimicking get_file_list)
    print("\n--- Attempt 2: With Session and Cookies ---")
    session = requests.Session()
    session.verify = False
    
    # Get cookies from home page
    home_url = "https://www.chinamoney.com.cn/chinese/qwjsn/"
    home_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    }
    try:
        print("Visiting home page...")
        session.get(home_url, headers=home_headers, timeout=10)
        print(f"Cookies: {session.cookies.get_dict()}")
        
        # Manually set cookies if needed (logic from get_file_list)
        if not session.cookies.get_dict():
            print("Setting manual cookies...")
            session.cookies.set("AlteonP10", "AYasNCw/F6zhU0YxOaADcg$$", domain="www.chinamoney.com.cn")
            session.cookies.set("lss", "953d30744145d363215192a47c98ceb5", domain="www.chinamoney.com.cn")
            session.cookies.set("isLogin", "0", domain="www.chinamoney.com.cn")

        # Now try download
        print("Downloading PDF with session...")
        # Update headers with Referer
        dl_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Referer": "https://www.chinamoney.com.cn/chinese/qwjsn/",
            # "Accept": "application/pdf" # Maybe not strictly needed or could be */*
        }
        r2 = session.get(url, headers=dl_headers, timeout=10)
        print(f"Status: {r2.status_code}")
        print(f"Content-Type: {r2.headers.get('Content-Type')}")
        if r2.status_code == 200:
             print("Success! (Content length: {})".format(len(r2.content)))
        else:
            print(f"Response (first 200 chars): {r2.text[:200]}")
            
    except Exception as e:
        print(f"Error in Attempt 2: {e}")

    # 3. Try with URL encoding (mimicking get_web_pdf_content)
    print("\n--- Attempt 3: With Incorrect Encoding (Mimicking Bug) ---")
    from urllib.parse import quote
    encoded_url = quote(url, safe=":/")
    print(f"Encoded URL: {encoded_url}")
    
    try:
        r3 = requests.get(encoded_url, headers=headers, verify=False, timeout=10)
        print(f"Status: {r3.status_code}")
        if r3.status_code == 404:
            print("Confirmed! Incorrect encoding causes 404.")
    except Exception as e:
        print(f"Error in Attempt 3: {e}")

if __name__ == "__main__":
    reproduction()
