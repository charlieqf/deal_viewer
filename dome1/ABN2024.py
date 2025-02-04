
# -*- coding: utf-8 -*-
import smtplib
from email.mime.text import MIMEText
import os
import re
from pypinyin import lazy_pinyin
import pandas as pd
import pymssql
import requests
import json
import time
import random
import ftplib
from datetime import datetime
import pyodbc
import chardet
import threading
import io
from dateutil.parser import parse

# FTP server details
FTP_HOST = "113.125.202.171"
FTP_PORT = 11421
FTP_USER = "dv"
FTP_PASS = "246qweASD@"
FTP_HOME_DIR = "DataTeam"
FTP_HOME_DIR = "/"

# FTP2 server details 192.168.1.211
FTP2_HOST = "113.125.202.171"
FTP2_PORT = 21121
FTP2_USER = "gsuser"
FTP2_PASS = "Password01"
FTP2_HOME_DIR = "."


UPDATE_LOG_PATH = "/Products_log/更新时间TXT记录/ABN更新时间sms.txt"
UPDATE_LOG_PATH_YYBG = "/Products_log/更新时间TXT记录/ABN更新时间sms_yybg.txt"
FTP_FOLDER_PATH = "/Products_log/银行间债券市场更新数据"
INCREMENT_FOLDER_PATH = "/增量文档"
DV_FOLDER_PATH = "/DealViewer/TrustAssociatedDoc"
DV_CREDIT_RATING_FOLDER = "ProductCreditRatingFiles"
DV_RELEASE_INSTRUCTION_FOLDER = "ProductReleaseInstructions"
DV_TRUSTEE_REPORT_FOLDER = "TrusteeReport"

current_dir = os.path.dirname(os.path.abspath(__file__))
cache_folder = os.path.join(current_dir, "abn_file_cache")
if not os.path.exists(cache_folder):
    os.makedirs(cache_folder)

def list_ftp_directory_with_retry(ftp, path, retries=6):
    for attempt in range(retries):
        try:
            return list_ftp_directory(ftp, path)
        except (ftplib.error_temp, ftplib.error_perm, ftplib.error_reply, BrokenPipeError) as e:
            print(
                f"Temporary error occurred: {e}. Retrying {retries - attempt - 1} more times."
            )
            time.sleep(5)  # Wait for 5 seconds before retrying

    raise Exception("Failed to list directory after multiple attempts")


def list_ftp_directory(ftp, path):
    """List files and directories in the given FTP path."""

    ftp.cwd(path)
    raw_items = []

    def handle_line(line):
        raw_items.append(line)

    ftp.retrlines("NLST", handle_line)

    raw_data = "\n".join(raw_items)

    # # Use retrbinary to get raw binary data
    # ftp.retrbinary("NLST", raw_items.append)

    # # Join the raw binary data and detect encoding
    # raw_data = b"".join(raw_items)
    detected_encoding = chardet.detect(raw_data.encode())["encoding"]
    if detected_encoding is None:
        detected_encoding = "utf-8"  # Fallback to utf-8 if detection fails
    #     print("Encoding detection failed, using utf-8 as fallback")
    # else:
    #     print(f"Detected encoding: {detected_encoding}")

    try:
        # Attempt to decode with detected encoding
        #items = raw_data.decode(detected_encoding).split("\r\n")
        items = raw_data.split("\n")
    except UnicodeDecodeError:
        # Fallback to utf-8 if detected encoding fails
        #print("Decoding with detected encoding failed, trying utf-8")
        items = raw_data.decode("utf-8", errors="ignore").split("\r\n")

    return items


def read_ftp_file(ftp, file_path):
    with io.BytesIO() as bio:
        ftp.retrbinary(f"RETR {file_path}", bio.write)
        bio.seek(0)
        return bio.read().decode("utf-8")


def enable_utf8(ftp):
    response = ftp.sendcmd("OPTS UTF8 ON")
    if "200" in response:
        print("UTF-8 encoding enabled on the FTP server.")
    else:
        print("Failed to enable UTF-8 encoding on the FTP server.")


def ftp1_keep_alive(interval=30, retries=6):
    while True:
        reconnect = False
        remaining_retries = retries
        while remaining_retries > 0:
            try:
                ftp.voidcmd("NOOP")
                reconnect = False
            except ftplib.all_errors as e:
                print(f"ftp1 Keep-alive error: {e}")
                reconnect = True
            time.sleep(interval)  # Keep-alive interval
            remaining_retries -= 1

        if reconnect:
            ftp.close()
            ftp.connect(FTP_HOST, FTP_PORT, timeout=600)
            ftp.login(FTP_USER, FTP_PASS)

def ftp2_keep_alive(interval=30, retries=6):
    while True:
        reconnect = False
        remaining_retries = retries
        while remaining_retries > 0:
            try:
                ftp2.voidcmd("NOOP")
                reconnect = False
            except ftplib.all_errors as e:
                print(f"ftp2 Keep-alive error: {e}")
                reconnect = True
            time.sleep(interval)  # Keep-alive interval
            remaining_retries -= 1

        if reconnect:
            ftp2.close()
            ftp2.connect(FTP2_HOST, FTP2_PORT, timeout=600)
            ftp2.login(FTP2_USER, FTP2_PASS)

def upload_file_to_ftp(ftp, local_file_path, ftp_folder, ftp_file_path, file_name):
    # if file does not exist on FTP, upload the file
    if file_name not in list_ftp_directory_with_retry(ftp, ftp_folder):
        print("Writing PDF to", ftp_file_path, "=====>")
        with open(local_file_path, "rb") as f:
            ftp.storbinary(f"STOR {ftp_file_path}", f)
    else:
        print("File already exists on FTP in folder:", ftp_folder)


# Connect to FTP server 10.0.0.114
ftp = ftplib.FTP()
ftp.connect(FTP_HOST, FTP_PORT, timeout=600)
ftp.login(FTP_USER, FTP_PASS)
# ftp.cwd(FTP_HOME_DIR)


# Connect to FTP server 192.168.1.211
ftp2 = ftplib.FTP()
ftp2.connect(FTP2_HOST, FTP2_PORT, timeout=600)
ftp2.login(FTP2_USER, FTP2_PASS)
enable_utf8(ftp2)
ftp2.encoding = "utf-8"

# Start a thread to keep the connection alive
keep_alive_thread = threading.Thread(
    target=ftp1_keep_alive, args=(30, 3)
)  # NOOP every 5 minutes
keep_alive_thread.daemon = True
keep_alive_thread.start()

keep_alive_thread2 = threading.Thread(
    target=ftp2_keep_alive, args=(30, 3)
)  # NOOP every 5 minutes
keep_alive_thread2.daemon = True
keep_alive_thread2.start()


def get_web_pdf_content_with_retry(web_pdf_path, retries=3):
    for attempt in range(retries):
        try:
            return get_web_pdf_content(web_pdf_path)
        except Exception as e:
            print(
                f"Error occurred while getting PDF content from {web_pdf_path}: {e}. Retrying {retries - attempt - 1} more times."
            )
            time.sleep(5)  # Wait for 5 seconds before retrying

    raise Exception("Failed to get PDF content after multiple attempts")

def get_web_pdf_content(web_pdf_path):
    encoded_url = quote(web_pdf_path, safe=":/")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        "Accept": "application/pdf",
    }
    # get the pdf file content from web (using proxy)
    # curl -x http://splci64blr:6j0z1hrwFM4LbdheZ_@gate.smartproxy.com:10001 -I https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/zjzczq_ABS/ABS_fxwj_ath/202405/P020240527386727954575.pdf -v
    response = requests.get(
        encoded_url, headers=headers, proxies=proxies, timeout=(10, 30)
    )

    # Check if the content is actually a PDF
    if response.headers.get("Content-Type") == "application/pdf":
        pdf_content = response.content
        return True, pdf_content
    else:
        return False, response.text


def create_dir_on_ftp(ftp, dir, folder):
    # check if the folder exists on the ftp server, if not, create the folder
    folder_path = os.path.join(dir, folder)
    if folder not in list_ftp_directory_with_retry(ftp, dir):
        print("create new folder: ", folder_path)
        ftp.mkd(folder_path)
    else:
        print(folder, "already exists in", dir, "on FTP")

    return folder_path

def get_sql_connection():
    try:
        # Establish connection using pyodbc
        conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=113.125.202.171,52482;"
            "Database=PortfolioManagement;"  # Use your database name
            "UID=sa;"  # Use your username
            "PWD=PasswordGS2017;"  # Use your password
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str)

        return conn
    except pyodbc.Error as e:
        print("Database error:", e)


conn = get_sql_connection()
print("got connection.")

# Smartproxy credentials and proxy URL
username = "splci64blr"
password = "6j0z1hrwFM4LbdheZ_"
proxy_url = f"http://{username}:{password}@gate.smartproxy.com:10001"
proxies = {
    "http": proxy_url,
    "https": proxy_url,
}

# conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
#                        database='PortfolioManagement', charset='utf8')
# cur = conn.cursor()

# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
#     "Accept-Language": "en-US,en;q=0.9",
#     "Accept-Encoding": "gzip, deflate, br",
#     # Add a referer if necessary, for instance, the main page or the previous page you'd access in a typical flow
#     "Referer": "https://www.chinamoney.com.cn/"
# }
static_url = 'https://www.chinamoney.com.cn/dqs/cm-s-notice-query/fileDownLoad.do?mode=open&contentId={}&priority=0'

def get_headers(url, use='pc'):
    pc_agent = [
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
        "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0);",
        "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
        "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
        "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"
        "Mozilla/5.0 (X11; Linux x86_64; rv:76.0) Gecko/20100101 Firefox/76.0"
    ]
    phone_agent = [
        "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
        "Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
        "Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
        "Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
        "MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
        "Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10",
        "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13",
        "Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 Mobile Safari/534.1+",
        "Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.0; U; en-US) AppleWebKit/534.6 (KHTML, like Gecko) wOSBrowser/233.70 Safari/534.6 TouchPad/1.0",
        "Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; HTC; Titan)",
        "UCWEB7.0.2.37/28/999",
        # "NOKIA5700/ UCWEB7.0.2.37/28/999",
        "Openwave/ UCWEB7.0.2.37/28/999",
        "Mozilla/4.0 (compatible; MSIE 6.0; ) Opera/UCWEB7.0.2.37/28/999"
    ]
    """user_agent部分来源:https://blog.csdn.net/IT__LS/java/article/details/78880903"""
    referer = lambda url: re.search(
        "^((https://)|(https://))?([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}(/)", url).group()
    """正则来源:https://www.cnblogs.com/blacksonny/p/6055357.html"""
    if use == 'phone':  # 随机选择一个
        agent = random.choice(phone_agent)
    else:
        agent = random.choice(pc_agent)

    headers = {
        'User-Agent': agent,
        'Referer': "https://www.chinamoney.com.cn/chinese/qwjsn/",
        "Cookie": "_ulta_id.CM-Prod.e9dc=9f8733fe96d3a061; AlteonP10=CLDtOSw/F6xDU59z7jzUGg$$; apache=4a63b086221745dd13be58c2f7de0338; ags=2a1ba4d47b619c011c19c1cc4b3c0c32; lss=fd9e664ef34511dcdc4a51a4e8d84abc; _ulta_ses.CM-Prod.e9dc=1187f9d14693de99; isLogin=0"
    }
    return headers

def get_proxy():
    return requests.get("http://162.14.106.158:5010/get/?type=http").json()

def delete_proxy(proxy):
    requests.get("http://162.14.106.158:5010/delete/?proxy={}".format(proxy))

def get_sign_and_info_level():
    baseurl = 'https://www.chinamoney.com.cn/lss/rest/cm-s-account/getLT'
    params = {'type': '0'}

    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        'Content-Length': '0',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': "https://www.chinamoney.com.cn/chinese/qwjsn/",
        "Cookie": "_ulta_id.CM-Prod.e9dc=9f8733fe96d3a061; AlteonP10=CLDtOSw/F6xDU59z7jzUGg$$; apache=4a63b086221745dd13be58c2f7de0338; ags=2a1ba4d47b619c011c19c1cc4b3c0c32; lss=fd9e664ef34511dcdc4a51a4e8d84abc; _ulta_ses.CM-Prod.e9dc=1187f9d14693de99; isLogin=0"
    }

    # headers = {
    #     "Accept": "*/*",
    #     "Accept-Encoding": "gzip, deflate, br, zstd",
    #     "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    #     "Cache-Control": "no-cache",
    #     "Connection": "keep-alive",
    #     "Content-Length": "0",
    #     "Cookie": "apache=bbfde8c184f3e1c6074ffab28a313c87; ags=a23ede7e97bccb1b2380be21609ada80; lss=f7cb2cf4b1607aec30e411e90d47c685; _ulta_id.CM-Prod.e9dc=857b1fa51afb521c; AlteonP10=Avl2Wiw/F6xv9116mHKodw$$; isLogin=0; _ulta_ses.CM-Prod.e9dc=43636e15f932ef74",
    #     "Host": "www.chinamoney.com.cn",
    #     "Origin": "https://www.chinamoney.com.cn",
    #     "Pragma": "no-cache",
    #     "Referer": "https://www.chinamoney.com.cn/chinese/qwjsn/?searchValue=%25E4%25B8%258A%25E5%25B8%2582%25E6%25B5%2581%25E9%2580%259A%2520ABN",
    #     "Sec-Fetch-Dest": "empty",
    #     "Sec-Fetch-Mode": "cors",
    #     "Sec-Fetch-Site": "same-origin",
    #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    #     "X-Requested-With": "XMLHttpRequest",
    #     "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    #     "sec-ch-ua-mobile": "?0",
    #     "sec-ch-ua-platform": '"Windows"'
    # }

    try:
        response = requests.post(baseurl, data=params, headers=headers, proxies=proxies)
        print(response)
        # 使用代理访问
        requestsdata = response.json()
        data = requestsdata['data']
        print("data =", data)

        info_level = data["UT"].replace('\n', '')
        sign = data["sign"].replace('\n', '')

        print("sign =", sign)
        print("info_level =", info_level)

        return sign, info_level

    except Exception as e:
        print("error getting sign and info_level:", e)
        return None, None
    
# 获取Sign和InfoLevel
def get_token():
    baseurl = 'https://www.chinamoney.com.cn/lss/rest/cm-s-account/getLT'
    params = {'type': '0'}

    # headers = {
    #     'User-Agent': 'Mozilla/5.0',
    #     'Content-Type': 'application/json',
    # }
    # # Define headers for the request
    # headers = {
    #     "Content-Type": "application/x-www-form-urlencoded",
    #     "Accept": "application/json, text/javascript, */*; q=0.01",
    #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    # }
    headers = get_headers('https://www.chinamoney.com.cn/chinese/qwjsn/?')
    retry_count = 5
    #proxy = get_proxy().get("proxy")
    while retry_count > 0:
        try:
            #response = requests.post(baseurl, data=params, headers=headers, proxies={"http": "http://{}".format(proxy)})
            response = requests.post(baseurl, data=params, headers=headers, proxies=proxies)
            # 使用代理访问
            requestsdata = response.json()
            data = requestsdata['data']
            print("data =", data)
            # 打印响应内容
            return data
        except Exception:
            retry_count -= 1
            # 删除代理池中代理
            #delete_proxy(proxy)
    return None

def get_html(timestamp):
    test_url = "https://ip.smartproxy.com/json"

    response = requests.get(test_url, proxies=proxies)
    print(response.status_code, response.json())

    token = get_token()
    print("token is " + str(token))
    info_level = token["UT"].replace('\n', '')
    sign = token["sign"].replace('\n', '')

    info_level = 'ZRUBESQi0sXmP3k%2Bbi3i16Hb4RXvja%2B%2BTrK4nH92M5eUlDo9QXVLURAq60SPVkidJmvkm7N1tEU7%0AwADYBlDg4KOuYpoh1tHsUAhVTaxIFUcHlilRLKrrkoNRH4F4NiGE8Izmqkm6QB4AK7sCu1ecS1PB%0A6eS0BmaiB/RHIjdGt9E=%0A'
    sign = 'Q/d8solfMh3GOoMI5WmGUaZA1ukiCpO5sMwap9ByMZnt4tsJZeSkX6Wq1v3lRrKsnQLcWdAPun00%0ALsYa5AtcTZpCs2CvuKf8xTKL5JKkAphGIIEbpsADAhjeg2dCZIBVMUOFd2LaiLvRLJLML9AfJTc/%0AI44XV2MvFkyyEBuTLsA=%0A'
    print("info_level:", info_level)
    print("sign:", sign)

    baseurl = 'https://www.chinamoney.com.cn/ses/rest/cm-u-notice-ses-cn/query?'

    params = {
        'sort': 'date',
        'text': '上市流通 ABN',
        'date': 'all',
        'field': 'title',
        'start': '',
        'end': '',
        'pageIndex': '1',
        'pageSize': '15',
        'public': 'false',
        'infoLevel': info_level,
        'sign': sign,
        'channelIdStr': '2496, 2556, 2632, 2663, 2589, 2850, 3300,',
        'nodeLevel': '1',
    }

    # Define headers for the request
    # headers = {
    #     "Content-Type": "application/x-www-form-urlencoded",
    #     "Accept": "application/json, text/javascript, */*; q=0.01",
    #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    # }
    # Define headers for the request
    # def get_headers(url):
    #     headers = {
    #         "Content-Type": "application/x-www-form-urlencoded",
    #         "Accept": "application/json, text/javascript, */*; q=0.01",
    #         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    #         "Referer": url,
    #     }
    #     return headers

    #headers = get_headers('https://www.chinamoney.com.cn/chinese/qwjsn/?')
    # Define headers for the request
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "0",
        "Cookie": "apache=bbfde8c184f3e1c6074ffab28a313c87; ags=a23ede7e97bccb1b2380be21609ada80; lss=f7cb2cf4b1607aec30e411e90d47c685; _ulta_id.CM-Prod.e9dc=857b1fa51afb521c; AlteonP10=Avl2Wiw/F6xv9116mHKodw$$; isLogin=0; _ulta_ses.CM-Prod.e9dc=43636e15f932ef74",
        "Host": "www.chinamoney.com.cn",
        "Origin": "https://www.chinamoney.com.cn",
        "Pragma": "no-cache",
        "Referer": "https://www.chinamoney.com.cn/chinese/qwjsn/?searchValue=%25E4%25B8%258A%25E5%25B8%2582%25E6%25B5%2581%25E9%2580%259A%2520ABN",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"'
    }    
    requests.packages.urllib3.disable_warnings()
    shortnameList = []
    urlList = []

    # load urlList from abn_urls.txt
    # each line is formatted as short_name:url:release_date
    with open('abn_urls.txt', 'r') as f:
        urlList = f.read().splitlines()

    # load shortnameList from urlList
    shortnameList = [i.split(':')[0] for i in urlList]

    print("there are already", len(urlList), "urls in the list")

    latestUpdateTime = datetime.strptime('2024-03-01', '%Y-%m-%d')

    for pageIndex in range(12, 16):
        print("fetching page", pageIndex)
        params['pageIndex'] = str(pageIndex)
        r = requests.post(baseurl, data=params, headers=headers, verify=False)

        # r = requests.post(baseurl, data=params, headers=headers, verify=False)

        #r = requests.post(baseurl, data=params, headers=headers, proxies=proxies)

        # 获取并打印状态码
        status_code = r.status_code
        print(f"HTTP Status Code: {status_code}")
        #print(r.text)
        

        # # 根据状态码进行判断或处理
        # if status_code == 403:
        #     proxy = get_proxy().get("proxy")
        #     r = requests.post(baseurl, data=params, headers=headers, verify=False, proxies={"http": "http://{}".format(proxy)})

        requestsdata = r.json()
        data = requestsdata['data']
        # print(data)
        result = data['result']
        pageItems = result['pageItems']

        firstTimestamp = pageItems[0]['releaseDate']

        for i in pageItems:
            release_date_ts = int(i['releaseDate']) / 1000
            release_date = datetime.fromtimestamp(release_date_ts)

            if release_date > latestUpdateTime:
                latestUpdateTime = release_date

            if release_date > parse(timestamp):
                title = i['title'][9:-1].replace("<font color='red'>", '').replace('</font>', '')
                short_name = re.findall('\d{2}.+?\d{3}', title)[0]
                print(short_name)
                if short_name not in shortnameList:
                    shortnameList.append(short_name)
                    urlList.append(short_name + ":" + i['dealPath'] + ":" + release_date.strftime('%Y-%m-%d'))
            else:
                break

        # sleep for a while
        time.sleep(random.uniform(1, 3))

    print(f'urlList is {urlList}')
    return latestUpdateTime, urlList

def post_list_data(query_url, url_data):
    retry_count = 3

    while retry_count > 0:
        try:
            proxy = get_proxy().get("proxy")
            print(proxy)
            headers = get_headers('https://www.chinamoney.com.cn/chinese/qwjsn/?')
            requests.packages.urllib3.disable_warnings()
            response = requests.post(query_url, params=url_data, headers=headers
                                     , proxies={"https": "https://{}".format(proxy), "http": "https://{}".format(proxy)})
            print(response.status_code)
            requestsdata = response.json()
            data = requestsdata['data']
            # 打印响应内容
            print(data)
            return data
        except Exception as e:
            print(e)
            retry_count -= 1
            # 删除代理池中代理
            delete_proxy(proxy)
            time.sleep(random.uniform(1, 5))
    return None

# 目前只抽取募集说明书与资产运营报告
def get_usefulPDF(pdf_list):
    q1 = []
    q2 = []
    exist = []
    p = re.compile('第.+?期')
    done = 0
    for i in pdf_list:
        if '募集说明书' in i[1] and done == 0:
            done = 1
            q1.append(i)
        if '发行' in i[1]:
            q1.append(i)
        # if '资产运营报告' in i[1]:
        #     q = p.findall(i[1].split('资产运营报告')[-1])
        #     if q:
        #         if q[-1] in exist:
        #             continue
        #         exist.append(q[-1])
        #         q2.append(i)
    return q1, q2


def get_file_list(product_name):
    print("getting file list for", product_name)

    info_level = 'Vg73kn1OepqOssdMjPsWQaNGpXyLBNdJ61g8NfkD7wDu4udG9ZIREUv+NEGY4UTLafJ9n2rDhA/vJl7ML6AHdhY4MfvnspH+srk3GZlFgg//y5tiv6RcS0POd+61cOsw6iNDaa74i3EeOa1lHSPhl6RGsBaXS5y2A1vPT30ouRo='
    sign = 'Q/d8solfMh3GOoMI5WmGUaZA1ukiCpO5sMwap9ByMZnt4tsJZeSkX6Wq1v3lRrKsnQLcWdAPun00LsYa5AtcTZpCs2CvuKf8xTKL5JKkAphGIIEbpsADAhjeg2dCZIBVMUOFd2LaiLvRLJLML9AfJTc/I44XV2MvFkyyEBuTLsA='

    url = 'https://www.chinamoney.com.cn/ses/rest/cm-u-notice-ses-cn/query'
    data = {'sort': 'date',
            'text': product_name,
            'date': 'all',
            'field': 'title',
            'start': '',
            'end': '',
            'pageIndex': '1',
            'pageSize': '15',
            'public': 'false',
            'infoLevel': info_level,
            'sign': sign,
            'channelIdStr': '2496, 2556, 2632, 2663, 2589, 2850, 3300,',
            'nodeLevel': '1'
    }
    # headers = get_headers('https://www.chinamoney.com.cn/chinese/qwjsn/?')
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "0",
        "Cookie": "apache=bbfde8c184f3e1c6074ffab28a313c87; ags=a23ede7e97bccb1b2380be21609ada80; lss=f7cb2cf4b1607aec30e411e90d47c685; _ulta_id.CM-Prod.e9dc=857b1fa51afb521c; AlteonP10=Avl2Wiw/F6xv9116mHKodw$$; isLogin=0; _ulta_ses.CM-Prod.e9dc=43636e15f932ef74",
        "Host": "www.chinamoney.com.cn",
        "Origin": "https://www.chinamoney.com.cn",
        "Pragma": "no-cache",
        "Referer": "https://www.chinamoney.com.cn/chinese/qwjsn/?searchValue=%25E4%25B8%258A%25E5%25B8%2582%25E6%25B5%2581%25E9%2580%259A%2520ABN",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"'
    }    

    rs = requests.post(url, data=data, headers=headers)
    time.sleep(4.33)
    print("rs.status =", rs.status_code)
    rs = json.loads(rs.text)
    total = rs['data']['result']['total']

    if total > 50:
        time.sleep(random.uniform(1.5, 2.9))
        data['pageSize'] = total
        rs = requests.post(url, data=data, headers=headers)
        time.sleep(3.156)
        rs = json.loads(rs.text)

    pdf_list = []
    pageItems = rs['data']['result']['pageItems']
    for i in pageItems:
        if len(i['paths']) == 1:
            z = re.search('\d{4}/?\d{2}/?\d{2}', i['paths'][0]).group().replace('/', '')
            date = z[:4] + '-' + z[4:6] + '-' + z[6:8] + ' 00:00:00.000'
            pdf_list.append([i['id'], i['title'].replace("<font color='red'>", '').replace("</font>", ''), date])
        else:
            print(len(i['paths']), i['id'], i['title'], 'XXX')
    return pdf_list


def get_url_list(timestamp):
    token = get_token()
    print("token is " + str(token))
    info_level = token["UT"].replace('\n', '')
    sign = token["sign"].replace('\n', '')

    query_url = 'https://www.chinamoney.com.cn/ses/rest/cm-u-notice-ses-cn/query'
    url_data = {
        'sort': 'date',
        'text': '上市流通 ABN',
        'date': 'all',
        'field': 'title',
        'start': '',
        'end': '',
        'pageIndex': '1',
        'pageSize': '15',
        'public': 'false',
        'infoLevel': info_level,
        'sign': sign,
        'channelIdStr': '2496,2556,2632,2663,2589,2850,3300,',
        'nodeLevel': '1'
    }

    data = post_list_data(query_url, url_data)

    pageItems = data['result']['pageItems']
    shortnameList = []
    urlList = []

    firstTimestamp = pageItems[0]['releaseDate']

    for i in pageItems:
        if i['releaseDate'] > timestamp:
            title = i['title'][9:-1].replace("<font color='red'>", '').replace('</font>', '')
            short_name = re.findall('\d{2}.+?\d{3}', title)[0]
            if short_name not in shortnameList:
                shortnameList.append(short_name)
                urlList.append(i['dealPath'])
        else:
            break

    print(f'urlList is {urlList}')
    return firstTimestamp, urlList


def upload_dir(path_source, session, target_dir=None):
    files = os.listdir(path_source)
    # 先记住之前在哪个工作目录中
    last_dir = os.path.abspath('..')
    # 然后切换到目标工作目录
    os.chdir(path_source)
    if target_dir:
        current_dir = session.pwd()
        try:
            session.mkd(target_dir)
        except Exception:
            pass
        finally:
            session.cwd(os.path.join(current_dir, target_dir))

    for file_name in files:
        current_dir = session.pwd()
        if os.path.isfile(path_source + r'/{}'.format(file_name)):
            upload_file(path_source, file_name, session)
        elif os.path.isdir(path_source + r'/{}'.format(file_name)):
            current_dir = session.pwd()
            try:
                session.mkd(file_name)
            except:
                pass
            session.cwd("%s/%s" % (current_dir, file_name))
            upload_dir(path_source + r'/{}'.format(file_name), session)
        # 之前路径可能已经变更，需要再回复到之前的路径里
        session.cwd(current_dir)
    os.chdir(last_dir)

def upload_folder_to_ftp(ftp, local_folder_path, ftp_folder, ftp_target_folder_name):
    # Change to the target directory, creating it if necessary
    ftp_folder_path = os.path.join(ftp_folder, ftp_target_folder_name).replace("\\", "/")
    try:
        ftp.cwd(ftp_folder_path)
    except ftplib.error_perm:
        ftp.mkd(ftp_folder_path)
        ftp.cwd(ftp_folder_path)

    for root, dirs, files in os.walk(local_folder_path):
        # Calculate the relative path
        rel_path = os.path.relpath(root, local_folder_path)
        ftp_path = os.path.join(ftp_folder_path, rel_path).replace("\\", "/")
        
        # Ensure directory exists on FTP
        parts = ftp_path.split('/')
        path_to_check = '/'
        for part in parts:
            if part:
                path_to_check = os.path.join(path_to_check, part).replace("\\", "/")
                try:
                    ftp.cwd(path_to_check)
                except ftplib.error_perm:
                    ftp.mkd(path_to_check)
                    ftp.cwd(path_to_check)

        # Upload files
        for file in files:
            local_file_path = os.path.join(root, file)
            ftp_file_path = os.path.join(ftp_path, file).replace("\\", "/")
            upload_file_to_ftp(ftp, local_file_path, ftp_file_path)

        # Reset back to initial target folder path
        ftp.cwd(ftp_folder_path)

def get_file_data_from_ftp(ftp, ftp_file_path):
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {ftp_file_path}", bio.write)
    bio.seek(0)
    return bio

def write_file_data_to_ftp(ftp, file_data, ftp_file_path):
    ftp.storbinary(f"STOR {ftp_file_path}", file_data)    

def upload_ftp_folder_to_ftp(ftp1, ftp2, ftp1_folder, ftp2_folder):
    for item in list_ftp_directory_with_retry(ftp1, ftp1_folder):
        ftp1_item_path = os.path.join(ftp1_folder, item).replace("\\", "/")
        ftp2_item_path = os.path.join(ftp2_folder, item).replace("\\", "/")

        # check if ftp1_item_path is a folder
        if '.' in ftp1_item_path:
            print(f"Transferring file: {ftp1_item_path} to {ftp2_item_path}")
            file_data = get_file_data_from_ftp(ftp1, ftp1_item_path)
            write_file_data_to_ftp(ftp2, file_data, ftp2_item_path)
        else:
            # Check if item is a directory on ftp1
            ftp1.cwd(ftp1_item_path)

            try:
                print(f"Creating directory: {ftp2_item_path}")
                ftp2.mkd(ftp2_item_path)
                ftp2.cwd(ftp2_item_path)
            except ftplib.error_perm:
                # Directory might already exist, ignore error
                pass
            # Recursively upload folder
            upload_ftp_folder_to_ftp(ftp1, ftp2, ftp1_item_path, ftp2_item_path)

        # try:
        #     # Check if item is a directory on ftp1
        #     ftp1.cwd(ftp1_item_path)
        #     ftp2.cwd(ftp2_item_path)
        # except ftplib.error_perm:
        #     # Item is a file
        #     print(f"Transferring file: {ftp1_item_path} to {ftp2_item_path}")
        #     file_data = get_file_data_from_ftp(ftp1, ftp1_item_path)
        #     write_file_data_to_ftp(ftp2, file_data, ftp2_item_path)
        # else:
        #     # Item is a directory
        #     print(f"Creating directory: {ftp2_item_path}")
        #     try:
        #         ftp2.mkd(ftp2_item_path)
        #     except ftplib.error_perm:
        #         # Directory might already exist, ignore error
        #         pass
        #     # Recursively upload folder
        #     upload_ftp_folder_to_ftp(ftp1, ftp2, ftp1_item_path, ftp2_item_path)

def upload_211(trustcode):
    month = str(datetime.now().month)
    folder = create_dir_on_ftp(ftp2, '/增量文档', month)
    source_folder = create_dir_on_ftp(ftp2, folder, trustcode)

    target_folder = create_dir_on_ftp(ftp2, '/DealViewer/TrustAssociatedDoc', trustcode)

    upload_ftp_folder_to_ftp(ftp, ftp2, source_folder, target_folder)

    # host = '192.168.1.211'
    # port = 21
    # username = 'gsuser'
    # password = 'Password01'
    # session = ftplib.FTP(host=host, user=username, passwd=password)
    # session.encoding = 'gbk'
    # upload_dir(os.path.join(r'\\172.16.7.114\/增量文档', str(datetime.now().month), trustcode), session,
    #            target_dir='DealViewer\TrustAssociatedDoc\{}'.format(trustcode))

def upload_211_bak(trustcode):
    host = '192.168.1.211'
    port = 21
    username = 'gsuser'
    password = 'Password01'
    session = ftplib.FTP(host=host, user=username, passwd=password)
    session.encoding = 'gbk'
    upload_dir(os.path.join(r'\\172.16.7.114\/增量文档', str(datetime.now().month), trustcode), session,
               target_dir='DealViewer\TrustAssociatedDoc\{}'.format(trustcode))


def crawl_pdf(product_name, i, type_f):
    pdf_file_name = i[1] + '.pdf'

    cache_pdf_path = os.path.join(cache_folder, pdf_file_name)
    file_exists = os.path.exists(cache_pdf_path)

    month = str(datetime.now().month)

    if not file_exists:
        url = static_url.format(int(i[0]))

        rst1 = 0
        while rst1 != 200:
            try:
                print("downloading file from url:", url)
                headers = get_headers('https://www.chinamoney.com.cn/chinese/qwjsn/?')
                print("download", pdf_file_name, "from", url)
                rs = requests.get(url, headers=headers)
                time.sleep(random.uniform(1, 10))
                rst1 = rs.status_code
                print("status code", rst1)
                if rst1 == 200:
                    with open(cache_pdf_path, "wb") as f:
                        f.write(rs.content)
                    file_exists = True

            except:
                print('downloading', pdf_file_name, 'failed')
                time.sleep(random.uniform(1, 5))

    if file_exists:
        ftp_folder = create_dir_on_ftp(ftp, '/Products/资产支持票据', product_name[2])
        ftp_file_path = os.path.join(ftp_folder, pdf_file_name)

        upload_file_to_ftp(ftp, cache_pdf_path, ftp_folder, ftp_file_path, pdf_file_name)

        if type_f == 1:
            create_dir_on_ftp(ftp, '/增量文档', month)
            folder = create_dir_on_ftp(ftp, f'/增量文档/{month}', product_name[1])
            ftp_folder = create_dir_on_ftp(ftp, folder, 'ProductReleaseInstructions')
            ftp_file_path = os.path.join(ftp_folder, pdf_file_name)
            upload_file_to_ftp(ftp, cache_pdf_path, ftp_folder, ftp_file_path, pdf_file_name)

        if type_f == 2:
            create_dir_on_ftp(ftp, '/增量文档', month)
            folder = create_dir_on_ftp(ftp, f'/增量文档/{month}', product_name[1])
            ftp_folder = create_dir_on_ftp(ftp, folder, 'TrusteeReport')
            ftp_file_path = os.path.join(ftp_folder, pdf_file_name)
            upload_file_to_ftp(ftp, cache_pdf_path, ftp_folder, ftp_file_path, pdf_file_name)

    # with open(r'\\172.16.7.114\/Products/资产支持票据\{}\{}.pdf'.format(product_name[2], i[1]), 'wb') as f:
    #     f.write(rs.content)
    #     print(i[1] + '.PDF done')

    # if type_f == 1:
    #     with open(
    #             r'\\172.16.7.114\/增量文档\{}\{}\ProductReleaseInstructions\{}.pdf'.format(
    #                 str(datetime.now().month), product_name[1],
    #                 i[1]),
    #             'wb') as f:
    #         f.write(rs.content)
    # if type_f == 2:
    #     with open(r'\\172.16.7.114\/增量文档\{}\{}\TrusteeReport\{}.pdf'.format(str(datetime.now().month),
    #                                                                                 product_name[1], i[1]),
    #               'wb') as f:
    #         f.write(rs.content)

    time.sleep(random.uniform(1.5, 2.9))

def crawl_pdf_bak(product_name, i, type_f):
    url = static_url.format(int(i[0]))

    rst1 = 0
    while rst1 != 200:
        try:
            headers = get_headers('https://www.chinamoney.com.cn/chinese/qwjsn/?')
            rs = requests.get(url, headers=headers)
            time.sleep(random.uniform(1, 10))
            rst1 = rs.status_code
        except:
            print('crawl_pdf failed')
            time.sleep(random.uniform(1, 5))
    with open(r'\\172.16.7.114\/Products/资产支持票据\{}\{}.pdf'.format(product_name[2], i[1]), 'wb') as f:
        f.write(rs.content)
        print(i[1] + '.PDF done')

    if type_f == 1:
        with open(
                r'\\172.16.7.114\/增量文档\{}\{}\ProductReleaseInstructions\{}.pdf'.format(
                    str(datetime.now().month), product_name[1],
                    i[1]),
                'wb') as f:
            f.write(rs.content)
    if type_f == 2:
        with open(r'\\172.16.7.114\/增量文档\{}\{}\TrusteeReport\{}.pdf'.format(str(datetime.now().month),
                                                                                    product_name[1], i[1]),
                  'wb') as f:
            f.write(rs.content)
    time.sleep(random.uniform(1.5, 2.9))


def upload_114(product_name, q1, q2):
    print(product_name)
    month = str(datetime.now().month)
    if any([q1, q2]):
        print("creating folder for", product_name[2])
        create_dir_on_ftp(ftp, '/Products/资产支持票据', product_name[2])
        folder = create_dir_on_ftp(ftp, '/增量文档', month)
        create_dir_on_ftp(ftp, folder, product_name[1])
        
        # if not os.path.exists(r'\\172.16.7.114\/Products/资产支持票据\{}'.format(product_name[2])):
        #     os.mkdir(r'\\172.16.7.114\/Products/资产支持票据\{}'.format(product_name[2]))
        # if not os.path.exists(r'\\172.16.7.114\/增量文档\{}\{}'.format(str(datetime.now().month), product_name[1])):
        #     os.mkdir(r'\\172.16.7.114\/增量文档\{}\{}'.format(str(datetime.now().month), product_name[1]))

    if q1:
        create_dir_on_ftp(ftp, f'/增量文档/{month}/{product_name[1]}', 'ProductReleaseInstructions')
        # if not os.path.exists((r'\\172.16.7.114\/增量文档\{}\{}\ProductReleaseInstructions'.format(str(datetime.now().month),
        #                                                                                  product_name[1]))):
        #     os.mkdir(r'\\172.16.7.114\/增量文档\{}\{}\ProductReleaseInstructions'.format(str(datetime.now().month),
        #                                                                                  product_name[1]))

    # if any([q1, q2]):
    #     os.mkdir(r'\\172.16.7.114\/Products/资产支持票据\{}'.format(product_name[2]))
    #     os.mkdir(r'\\172.16.7.114\/增量文档\{}\{}'.format(str(datetime.now().month), product_name[1]))
    #
    # if q1:
    #     os.mkdir(r'\\172.16.7.114\/增量文档\{}\{}\ProductReleaseInstructions'.format(str(datetime.now().month),
    #                                                                                      product_name[1]))
        for i in q1:
            print(i)
            crawl_pdf(product_name, i, 1)
    # if q2:
    #     os.mkdir(r'\\172.16.7.114\/增量文档\{}\{}\TrusteeReport'.format(str(datetime.now().month), product_name[1]))
    #     for i in q2:
    #         print(i)
    #         crawl_pdf(product_name, i, 2)

    # for i in q1:
    #     print(i)
    #     crawl_pdf(product_name, i, 1)

def newProd(url):
    print('process', url)
    df = pd.DataFrame()
    url = 'https://www.chinamoney.com.cn' + url

    with open('url_done.txt', 'r', encoding='utf8') as f:
        done_urls = f.read().splitlines()
        if url in done_urls:
            print('文件判断此url已处理完成')
            return

    flag = 0

    retries = 5

    while flag == 0 and retries > 0:
        try:
            retries -= 1
            wd = pd.read_html(url)
            flag = 1
        except:
            print('pd.read_html failed')
            time.sleep(random.uniform(1, 5))

    df = pd.concat(wd, ignore_index=True)
    fullName = df.iloc[1][1]
    fullName = fullName.split('票据')[0] + '票据'
    shortName = df.iloc[1][3]
    shortName = shortName[2:shortName.find('ABN') + 3] + '20' + shortName[:2] + '-' + str(
        int(shortName[shortName.find('ABN') + 3:shortName.find('ABN') + 6]))
    offer_type = df.iloc[-1][3]

    if '一汽' not in fullName:
        return

    # 公募：PublicOffering，私募：PrivateEquity，其它：Others
    if offer_type == '公开发行':
        CollectionMethod = 'PublicOffering'
    elif offer_type == '定向发行':
        CollectionMethod = 'PrivateEquity'
    else:
        CollectionMethod = 'Others'

    # # conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
    # #                        database='PortfolioManagement', charset='utf8')
    cursor = conn.cursor()

    TrustStatus = 'Duration'
    TrustName = fullName
    TrustNameShort = shortName
    TrustCode = ''.join([i[0].upper() + i[1:] for i in lazy_pinyin(TrustNameShort)])

    # check if TrustName exists in database_existing_products.txt
    # trust_exists = False
    # existing_file_path = os.path.join(current_dir, 'database_existing_products.txt')
    # if os.path.exists(existing_file_path):
    #     with open('database_existing_products.txt', 'r', encoding='utf8') as f:
    #         existing_products = f.read().splitlines()
    #         if TrustName in existing_products:
    #             trust_exists = True
    #             print(TrustName, '文件判断数据库已存在')

    #             with open('existing_urls.txt', 'a', encoding='utf8') as f:
    #                 f.write(url + '\n')

                #return


    isExist = "select 1 from TrustManagement.Trust where TrustName=N'{}' or TrustCode='{}' or TrustNameShort=N'{}'".format(
        TrustName, TrustCode, TrustNameShort)

    cursor.execute(isExist)
    res = cursor.fetchone()
    if res:
        print(TrustName, '数据库已存在')

        with open('核查冲突产品.txt', 'a', encoding='utf8') as f:
            f.write(df.iloc[1][3] + ' ')
            f.write(TrustName + ' ')
            f.write(TrustCode + ' ')
            f.write(TrustNameShort + '\n')
        #return
    else:
        selectTrustId = "select max(TrustId)+1 from TrustManagement.Trust where TrustId<50000"
        cursor.execute(selectTrustId)
        TrustId = cursor.fetchone()[0]

        # 插入Trust数据
        insertTrust = '''
            SET IDENTITY_INSERT TrustManagement.Trust ON ;
            insert into TrustManagement.Trust(TrustId,TrustCode,TrustName,TrustNameShort,IsMarketProduct,TrustStatus) values({},'{}',N'{}',N'{}',1,'Duration');
            SET IDENTITY_INSERT TrustManagement.Trust OFF ;
        '''.format(TrustId, TrustCode, TrustName, TrustNameShort)
        print(insertTrust)

        try:
            cursor.execute(insertTrust)
            conn.commit()
            print(TrustName, '基础表信息插入完成!')
        except Exception as e:
            print('err1', e)
            # print(TrustName)
            # print(insertTrust)
            return

        # 插入同步表数据
        insertFATrust = '''
            SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust ON ;
            insert into FixedIncomeSuite.Analysis.Trust(TrustId,TrustCode,TrustName) values({},'{}',N'{}');
            SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust OFF ;
        '''.format(TrustId, TrustCode, TrustName)
        print(insertFATrust)

        try:
            cursor.execute(insertFATrust)
            conn.commit()
            print(TrustName, '同步表信息插入完成!')
        except Exception as e:
            print('errFA', e)
            # print(TrustName)
            # print(insertFATrust)

        # 插入TrustInfoExtension数据
        RegulatoryOrg = 'NAFMII'
        MarketPlace = 'InterBank'

        TrustInfoExtension = "insert into TrustManagement.TrustInfoExtension(TrustId, StartDate, EndDate, ItemId, ItemCode, ItemValue) values " \
                            "({}, GETDATE(), null, null, 'MarketCategory','ABN')," \
                            "({}, GETDATE(), null, null, 'RegulatoryOrg','{}')," \
                            "({}, GETDATE(), null, null, 'MarketPlace', '{}')," \
                            "({}, GETDATE(), null, null, 'AssetType',null)," \
                            "({}, GETDATE(), null, null, 'BasicAssetType',null)," \
                            "({}, GETDATE(), null, null, 'CollectionMethod', '{}');".format(
            TrustId, TrustId, RegulatoryOrg, TrustId, MarketPlace, TrustId, TrustId, TrustId, CollectionMethod)

        try:
            cursor.execute(TrustInfoExtension)
            cursor.execute("update TrustManagement.TrustInfoExtension set ItemValue=null where ItemValue='nan'")
            conn.commit()
            print(TrustId, TrustName, 'TrustInfoExtension表数据插入完成!')
        except Exception as e:
            print('err2', e)
            print(TrustName)
            print(TrustInfoExtension)
            return

    sql = f"select TrustId from TrustManagement.Trust where TrustName = N'{TrustName}'"
    cursor.execute(sql)
    TrustId = cursor.fetchone()[0]
    
    if CollectionMethod == 'PublicOffering':
        product_name = [TrustId, TrustCode, TrustName]
        file_list = get_file_list(TrustName)
        q1, q2 = get_usefulPDF(file_list)
        print(q1, q2)
        upload_114(product_name, q1, q2)
        if q1:
            smsExist = 0
            upload_211(product_name[1])
            for q in q1:
                insert_into_db(product_name, q, 1)
                if '说明书' in q[1]:
                    smsExist = 1
            if not smsExist:
                gmEx.add(product_name[2])
    else:
        print("非公开发行")

    with open('url_done.txt', 'a', encoding='utf8') as f:
        f.write(url + '\n')

    return offer_type, TrustName

def insert_into_db(product_name, q, typy_q):
    cursor = conn.cursor()
    #sql_i = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values(%s,%s,'NULL',%s,%s,'pdf',%s,'yixianfeng')".format()
    Trustid = product_name[0]
    TrustCode = product_name[1]
    if typy_q == 1:
        FileCategory = 'ProductReleaseInstructions'
        FilePath = 'DealViewer/TrustAssociatedDoc/{}/{}/'.format(TrustCode, FileCategory)
    if typy_q == 2:
        FileCategory = 'TrusteeReport'
        FilePath = 'DealViewer/TrustAssociatedDoc/{}/{}/'.format(TrustCode, FileCategory)

    file_name = q[1] + '.pdf'
    created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    sql_i = f"insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values({Trustid}, '{FileCategory}','NULL','{FilePath}',N'{file_name}','pdf','{created}','goldenstand')"

    print(sql_i)
    cursor.execute(sql_i)
    # cursor.execute(sql_i, (
    #     Trustid, FileCategory, FilePath, q[1] + '.pdf', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    print('asd')
    print(q)
    print(q[1])
    print('asd')
    if '募集说明书' in q[1]:
        # sql_i1 = "insert into PortfolioManagement.dbo.DisclosureOfInformation values(%s,%s,%s,%s,%s)".format(
        # )
        # # 之前记录披露时间，现在改成跟文档表一样的入库时间
        # cursor.execute(sql_i1,
        #                (Trustid, TrustCode, q[1], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), typy_q))

        sql_i1 = f"insert into PortfolioManagement.dbo.DisclosureOfInformation values({Trustid}, '{TrustCode}', N'{q[1]}', '{created}', {typy_q})"
        print(sql_i1)
        cursor.execute(sql_i1)

        sql = f"select TrustDocumentID from DV.TrustAssociatedDocument where filename=N'{q[1]}.pdf'"
        cursor.execute(sql)
        # cursor.execute('select TrustDocumentID from DV.TrustAssociatedDocument where filename=%s', q[1] + '.pdf')
        tdid = cursor.fetchone()[0]

        # sql_i2 = "insert into TaskCollection.dbo.ProductsStateInformation(Trustid,TrustDocumentID,FileType,StateType) values(%s,%s,%s,9)".format()
        # cursor.execute(sql_i2, (Trustid, tdid, typy_q))
        sql_i2 = f"insert into TaskCollection.dbo.ProductsStateInformation(Trustid,TrustDocumentID,FileType,StateType) values({Trustid}, {tdid}, {typy_q}, 9)"
        print(sql_i2)
        cursor.execute(sql_i2)

    conn.commit()
    #conn.close()
    print('single insert done')


def insert_db(product_name, q, typy_q):
    server = "172.16.6.143\mssql"
    user = "sa"
    password = "PasswordGS2017"
    database = "PortfolioManagement"
    conn = pymssql.connect(server, user, password, database)
    cursor = conn.cursor()
    sql_i = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values(%s,%s,'NULL',%s,%s,'pdf',%s,'yixianfeng')".format(
    )
    Trustid = product_name[0]
    TrustCode = product_name[1]
    if typy_q == 1:
        FileCategory = 'ProductReleaseInstructions'
        FilePath = 'DealViewer/TrustAssociatedDoc/{}/{}/'.format(TrustCode, FileCategory)
    if typy_q == 2:
        FileCategory = 'TrusteeReport'
        FilePath = 'DealViewer/TrustAssociatedDoc/{}/{}/'.format(TrustCode, FileCategory)

    cursor.execute(sql_i, (
        Trustid, FileCategory, FilePath, q[1] + '.pdf', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    print('asd')
    print(q)
    print(q[1])
    print('asd')
    if '募集说明书' in q[1]:
        sql_i1 = "insert into PortfolioManagement.dbo.DisclosureOfInformation values(%s,%s,%s,%s,%s)".format(
        )
        # 之前记录披露时间，现在改成跟文档表一样的入库时间
        cursor.execute(sql_i1,
                       (Trustid, TrustCode, q[1], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), typy_q))
        sql_i2 = "insert into TaskCollection.dbo.ProductsStateInformation(Trustid,TrustDocumentID,FileType,StateType) values(%s,%s,%s,9)".format(
        )
        cursor.execute('select TrustDocumentID from DV.TrustAssociatedDocument where filename=%s', q[1] + '.pdf')
        tdid = cursor.fetchone()[0]
        cursor.execute(sql_i2, (Trustid, tdid, typy_q))

    conn.commit()
    #conn.close()
    print('single insert done')


def get_url_list1(timestamp):
    test_url = "https://ip.smartproxy.com/json"

    response = requests.get(test_url, proxies=proxies)
    print(response.status_code, response.json())

    shortnameList = []
    urlList = []

    # load urlList from abn_urls.txt
    # each line is formatted as short_name:url:release_date
    with open('abn_yybg.txt', 'r') as f:
        urlList = f.read().splitlines()

    # load shortnameList from urlList
    shortnameList = [i.split(':')[0] for i in urlList]

    # token = get_token()
    # print("token is " + str(token))
    # info_level = token["UT"].replace('\n', '')
    # sign = token["sign"].replace('\n', '')

    #sign, info_level = get_sign_and_info_level()
    info_level = 'Z+WGvV60F9jW6cApNBWHlBok4r/b3zAP7Qpxp/E7O7L1WKV0c41RLsT8wXheqUhDKbV7uaJHsEvs\n/Afj43n/Y1Z4saFkrmkimb5ZRs1e4Z02Rw9II5+csig/YHm+A9FfH4b5Mis4tQUP6Gkuve0jpdA8\n8nUihNf8QIJCguXf62U=\n'
    sign = 'Q/d8solfMh3GOoMI5WmGUaZA1ukiCpO5sMwap9ByMZnt4tsJZeSkX6Wq1v3lRrKsnQLcWdAPun00\nLsYa5AtcTZpCs2CvuKf8xTKL5JKkAphGIIEbpsADAhjeg2dCZIBVMUOFd2LaiLvRLJLML9AfJTc/\nI44XV2MvFkyyEBuTLsA=\n'

    # headers = {
    #     'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    #     'Content-Length': '0',
    #     'Accept': '*/*',
    #     'Accept-Encoding': 'gzip, deflate, br',
    #     'Connection': 'keep-alive',
    #     'Referer': "https://www.chinamoney.com.cn/chinese/qwjsn/",
    #     "Cookie": "apache=bbfde8c184f3e1c6074ffab28a313c87; ags=a23ede7e97bccb1b2380be21609ada80; lss=f7cb2cf4b1607aec30e411e90d47c685; _ulta_id.CM-Prod.e9dc=857b1fa51afb521c; AlteonP10=CEEzVyw/F6xCWyg1V3BoKg$$; _ulta_ses.CM-Prod.e9dc=be68a782ad3bc8f4"
    # }

    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "0",
        "Cookie": "apache=bbfde8c184f3e1c6074ffab28a313c87; ags=a23ede7e97bccb1b2380be21609ada80; lss=f7cb2cf4b1607aec30e411e90d47c685; _ulta_id.CM-Prod.e9dc=857b1fa51afb521c; AlteonP10=CEEzVyw/F6xCWyg1V3BoKg$$; _ulta_ses.CM-Prod.e9dc=be68a782ad3bc8f4",
        "Host": "www.chinamoney.com.cn",
        "Origin": "https://www.chinamoney.com.cn",
        "Pragma": "no-cache",
        "Referer": "https://www.chinamoney.com.cn/chinese/qwjsn/?searchValue=%25E4%25B8%258A%25E5%25B8%2582%25E6%25B5%2581%25E9%2580%259A%2520ABN",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"'
    }

    requests.packages.urllib3.disable_warnings()     
    
    # print("test....")
    # query_url = 'https://www.chinamoney.com.cn/ses/rest/cm-u-notice-ses-cn/query'
    # url_data = {'sort': 'date',
    #             'text': '运营报告',
    #             'date': 'all',
    #             'field': 'title',
    #             'start': '',
    #             'end': '',
    #             'pageIndex': '1',
    #             'pageSize': '15',
    #             'public': 'false',
    #             'infoLevel': info_level,
    #             'sign': sign,
    #             'channelIdStr': '2496, 2556, 2632, 2663, 2589, 2850, 3300,',
    #             'nodeLevel': '1'}

    # rs = requests.post(query_url, data=url_data, headers=headers, proxies=proxies)
    
    # print("status code:", rs.status_code, "status text:", rs.text)
    latestUpdateTime = datetime.strptime('2023-01-01', '%Y-%m-%d')

    for pageIndex in range(5, 11):
        query_url = 'https://www.chinamoney.com.cn/ses/rest/cm-u-notice-ses-cn/query'
        url_data = {'sort': 'date',
                    'text': '运营报告',
                    'date': 'all',
                    'field': 'title',
                    'start': '',
                    'end': '',
                    'pageIndex': str(pageIndex),
                    'pageSize': '15',
                    'public': 'false',
                    'infoLevel': info_level,
                    'sign': sign,
                    'channelIdStr': '2496, 2556, 2632, 2663, 2589, 2850, 3300,',
                    'nodeLevel': '1'}
        rst = 0
        while rst != 200:
            try:
                # headers = get_headers('https://www.chinamoney.com.cn/chinese/qwjsn/?')
                print("posting to", query_url)
                rs = requests.post(query_url, data=url_data, headers=headers, proxies=proxies)
                rst = rs.status_code
                if rst != 200:
                    print("status code:", rs.status_code, "status text:", rs.text)
            except:
                print('get_url_list1 failed')
                time.sleep(random.uniform(1, 5))

        rs = json.loads(rs.text)

        pageItems = rs['data']['result']['pageItems']
        print(pageItems)

        firstTimestamp = datetime.fromtimestamp(int(pageItems[0]['releaseDate']) / 1000)
        if firstTimestamp > latestUpdateTime:
            latestUpdateTime = firstTimestamp

        # save pageItems to abn_yybg.txt so it can be loaded later
        with open('abn_yybg.txt', 'w') as f:
            f.write(json.dumps(pageItems))

        cur = conn.cursor()

        print("正在处理第", pageIndex, "页")

        for i in pageItems:
            release_date_ts = int(i['releaseDate']) / 1000
            release_date = datetime.fromtimestamp(release_date_ts)

            if release_date > parse(timestamp):
                title = i['title'].replace("<font color='red'>", '').replace('</font>', '')

                if '票据' not in title:
                    continue
                
                print(title)

                done_file_path = os.path.join(cache_folder, title + '.done')

                # if os.path.exists(done_file_path):
                #     continue

                prod = title.split('票据')[0] + '票据'
                if '《' in prod:
                    prod = prod.split('《')[-1]
                sub_title = title.split('票据')[1]
                if '年度' not in sub_title and '第' in sub_title and '期' in sub_title:
                    sql = "select trustid,trustcode from TrustManagement.Trust where trustname = N'{}'".format(prod)
                    cur.execute(sql)
                    res = cur.fetchone()
                    if res:
                        sqlQ = "select 1 from DV.TrustAssociatedDocument where filename = N'{}'".format(title)
                        cur.execute(sqlQ)
                        resQ = cur.fetchone()
                        if resQ:
                            print(title, ' 文档已存在')
                            existFile.append(title)
                            continue

                        tid = res[0]
                        product_code = res[1]

                        month = str(datetime.now().month)
                        # folder = create_dir_on_ftp(ftp, '/增量文档', month)
                        folder = create_dir_on_ftp(ftp, f'/增量文档/{month}', product_code)
                        create_dir_on_ftp(ftp, folder, 'TrusteeReport')
                        create_dir_on_ftp(ftp, '/Products/资产支持票据', prod)

                        # pathZl = r'\\172.16.7.114\/增量文档\{}\{}\TrusteeReport'.format(str(datetime.now().month),
                        #                                                                     product_code)
                        # if not os.path.exists(pathZl):
                        #     os.makedirs(pathZl)

                        # path114 = r'\\172.16.7.114\/Products/资产支持票据\{}'.format(prod)
                        # if not os.path.exists(path114):
                        #     os.mkdir(path114)

                        fid = i['id']
                        # 将pdf下载到114,增量中
                        crawl_pdf1(prod, product_code, title, fid)
                        # 插入三张表的记录
                        if '关于' in title or '更新' in title or '更正' in title:
                            updateFile.append(title)
                        elif not checkDate(tid):
                            updateFile.append(title)
                        else:
                            insertDB(tid, product_code, title)
                            print(prod, title, ' 114增量处理完毕')
                            upload_211(product_code)
                            print(prod, title, ' 211处理完毕')
                            succFile.append(title)

                        with open(done_file_path, "w") as f:
                            f.write("Done")

                    else:
                        print(title, ' 没有找到对应产品')
                        failFile.append(title)
                        continue
            else:
                break

    return latestUpdateTime


def checkDate(id):
    sql = 'select max(convert(datetime, DisclosureTime))+20 from PortfolioManagement.dbo.DisclosureOfInformation ' \
          'where trustid = {} and filetype = 2'.format(id)
    print(sql)
    cur = conn.cursor()
    cur.execute(sql)
    res = cur.fetchone()[0]
    if res and datetime.now() < res:
        return False
    return True

def insertDB(id, code, filename):
    sql1 = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) " \
           "values ({},'TrusteeReport',NULL,'DealViewer/TrustAssociatedDoc/{}/TrusteeReport/',N'{}.pdf','pdf',getdate(),'yixianfeng')".format(
        id, code, filename)

    sql2 = "insert into PortfolioManagement.dbo.DisclosureOfInformation values" \
           " ({}	,'{}',N'{}',	getdate(),	2)".format(id, code, filename)
    try:
        cur = conn.cursor()
        cur.execute(sql1)
        cur.execute(sql2)
        cur.execute("select trustdocumentid from DV.TrustAssociatedDocument where filename=N'{}.pdf'".format(filename))
        tid = cur.fetchone()[0]
        sql3 = "insert into TaskCollection.dbo.ProductsStateInformation(Trustid,TrustDocumentID,FileType,StateType)" \
               " values ({},{},2,9)".format(id, tid)
        cur.execute(sql3)
        conn.commit()
    except:
        print(filename)


def upload_file(path, file_name, session, target_dir=None, callback=None):
    # 记录当前 ftp 路径
    cur_dir = session.pwd()
    if target_dir:
        try:
            session.mkd(target_dir)
        except:
            pass
        finally:
            session.cwd(os.path.join(cur_dir, target_dir))

    print("path:%s \r\n\t   file_name:%s" % (path, file_name))
    file = open(os.path.join(path, file_name), 'rb')  # file to send
    session.storbinary('STOR %s' % file_name, file, callback=callback)  # send the file
    file.close()  # close file
    session.cwd(cur_dir)



def crawl_pdf1(product_name, product_code, filename, fid):
    pdf_file_name = filename + '.pdf'
    cache_pdf_path = os.path.join(cache_folder, pdf_file_name)

    success_file_path = os.path.join(cache_folder, f"{filename}.success")

    # if 'product_title.success' in cache folder, skip
    if os.path.exists(success_file_path):
        #print(product["title"], "already processed")
        return

    file_exists = os.path.exists(cache_pdf_path)

    month = str(datetime.now().month)

    if not file_exists:
        url = static_url.format(fid)
        rst1 = 0
        while rst1 != 200:
            try:
                print("downloading file from url:", url)
                headers = get_headers('https://www.chinamoney.com.cn/chinese/qwjsn/?')
                rs = requests.get(url, headers=headers)
                time.sleep(random.uniform(1, 10))
                rst1 = rs.status_code

                if rst1 == 200:
                    with open(cache_pdf_path, "wb") as f:
                        f.write(rs.content)
                    file_exists = True
            except:
                print('download', pdf_file_name, 'failed')
                time.sleep(random.uniform(1, 5))

    if file_exists:
        ftp_folder = create_dir_on_ftp(ftp, '/Products/资产支持票据', product_name)
        ftp_file_path = os.path.join(ftp_folder, pdf_file_name)

        upload_file_to_ftp(ftp, cache_pdf_path, ftp_folder, ftp_file_path, pdf_file_name)

        folder = create_dir_on_ftp(ftp, '/增量文档', month)
        folder = create_dir_on_ftp(ftp, folder, product_code)
        ftp_folder = create_dir_on_ftp(ftp, folder, 'TrusteeReport')
        ftp_file_path = os.path.join(ftp_folder, pdf_file_name)
        upload_file_to_ftp(ftp, cache_pdf_path, ftp_folder, ftp_file_path, pdf_file_name)

        # with open(r'\\172.16.7.114\/Products/资产支持票据\{}\{}.pdf'.format(product_name, filename), 'wb') as f:
        #     f.write(rs.content)
        #     print(filename + '.PDF done')

        # with open(r'\\172.16.7.114\/增量文档\{}\{}\TrusteeReport\{}.pdf'.format(str(datetime.now().month),
        #                                                                             product_code, filename), 'wb') as f:
        #     f.write(rs.content)

        with open(success_file_path, "w") as f:
            f.write("Success")

    time.sleep(random.uniform(1.5, 9.9))

def crawl_pdf1_bak(product_name, product_code, filename, fid):
    url = static_url.format(fid)
    rst1 = 0
    while rst1 != 200:
        try:
            headers = get_headers('https://www.chinamoney.com.cn/chinese/qwjsn/?')
            rs = requests.get(url, headers=headers)
            time.sleep(random.uniform(1, 10))
            rst1 = rs.status_code
        except:
            print('crawl_pdf1 failed')
            time.sleep(random.uniform(1, 5))
    with open(r'\\172.16.7.114\/Products/资产支持票据\{}\{}.pdf'.format(product_name, filename), 'wb') as f:
        f.write(rs.content)
        print(filename + '.PDF done')

    with open(r'\\172.16.7.114\/增量文档\{}\{}\TrusteeReport\{}.pdf'.format(str(datetime.now().month),
                                                                                product_code, filename), 'wb') as f:
        f.write(rs.content)
    time.sleep(random.uniform(1.5, 9.9))


def mail(gmProds, smProds, qtProds, gmEx, succFile, failFile, existFile, updateFile):
    msg_from = 'fengyanyan@goldenstand.cn'  # 发送方邮箱
    passwd = 'Fyy2516302813'  # 填入发送方邮箱的授权码
    msg_to1 = ['fengyanyan@goldenstand.cn',
               'qfeng@goldenstand.cn', 'zhangtongyao@gdsd.wecom.work']
    htmlText = '''<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Title</title>
    </head>
    <body>
    <u>{}</u>'''.format(time.strftime("%Y-%m-%d"))

    if any((gmProds, smProds, qtProds, gmEx)):
        cnt = len(gmProds) + len(smProds) + len(qtProds)
        htmlText += '''<div style="color:blue">新增ABN产品:  {} 个</div>'''.format(cnt) + '<pre>'
        if gmProds:
            htmlText += '  公募产品：{} 个\n    '.format(len(gmProds)) + '\n    '.join(gmProds) + '\n'
        if smProds:
            htmlText += '  私募产品：{} 个\n    '.format(len(smProds)) + '\n    '.join(smProds) + '\n'
        if qtProds:
            htmlText += '  其他产品：{} 个\n    '.format(len(qtProds)) + '\n    '.join(qtProds) + '\n'
        if gmEx:
            htmlText += '  公募但没有说明书：{} 个\n    '.format(len(gmEx)) + '\n    '.join(gmEx) + '\n'
        htmlText += '</pre>'

    if any((succFile, failFile, existFile, updateFile)):
        cnt = len(succFile) + len(failFile) + len(updateFile)
        htmlText += '''<div style="color:blue">新增ABN运营报告:  {} 份</div>'''.format(cnt) + '<pre>'
        if succFile:
            htmlText += '  成功入库报告：{} 份\n    '.format(len(succFile)) + '\n    '.join(succFile) + '\n'
        if failFile:
            htmlText += '  无对应产品报告：{} 份\n    '.format(len(failFile)) + '\n    '.join(failFile) + '\n'
        if updateFile:
            htmlText += '  更新报告以及20天之内重复入库报告：{} 份\n    '.format(len(updateFile)) + '\n    '.join(updateFile) + '\n'
        if existFile:
            htmlText += '  已存在报告：{} 份\n    '.format(len(existFile)) + '\n    '.join(existFile) + '\n'
        htmlText += '</pre>'

    htmlText += '''</body></html>'''

    for msg_to in msg_to1:
        subject = "ABN新增文件情况 "  # 主题
        msg = MIMEText(htmlText, 'html', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = msg_from
        msg['To'] = msg_to
        try:
            s = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
            s.login(msg_from, passwd)
            s.sendmail(msg_from, msg_to, msg.as_string())
            print("发送成功")
        except:
            print("发送失败")
        finally:
            s.quit()

    print('发送完毕')

if __name__ == '__main__':
    
    timestamp = read_ftp_file(ftp, UPDATE_LOG_PATH)
    print("上次更新的日期为 " + timestamp)
    # timestamp = '2024-01-01 15:45:02'
    # with open(r'E:\脚本\ABN每周跑批入库\lastTime_sms.txt') as f:
    #     timestamp = int(f.read())
    # print(timestamp)

    firstTimestamp, urls = get_html(timestamp)
    
    # with open('abn_urls.txt', 'r') as f:
    #     urls = f.read().split('\n')
    
    # save urls in abn_urls.txt line by line, overwriting the file
    with open('abn_urls.txt', 'w') as f:
        for url in urls:
            f.write(url + '\n')

    gmProds, smProds, qtProds = [], [], []
    gmEx = set()

    for url in urls:
        if len(url.split(':')) < 3:
            continue

        url = url.split(':')[1]
        res = newProd(url)
        if res:
            if res[0] == '公开发行':
                gmProds.append(res[1])
            elif res[0] == '定向发行':
                smProds.append(res[1])
            else:
                qtProds.append(res[1])

    # #发送邮件
    # mail(gmProds, smProds, qtProds, gmEx)
    

    # 更新时间
    print("Writing latest date time {} to".format(firstTimestamp), UPDATE_LOG_PATH)
    firstTimestamp_str = firstTimestamp.strftime("%Y-%m-%d %H:%M:%S")

    with io.BytesIO(firstTimestamp_str.encode("utf-8")) as bio:
        ftp.storbinary(f"STOR {UPDATE_LOG_PATH}", bio)
    # if urls:
    #     with open(r'E:\脚本\ABN每周跑批入库\lastTime_sms.txt', 'w') as f:
    #         f.write(str(firstTimestamp))
    
    # yybg
    timestamp = read_ftp_file(ftp, UPDATE_LOG_PATH_YYBG)
    print("上次更新的日期为 " + timestamp)

    # with open(r'E:\脚本\ABN每周跑批入库\lastTime_yybg.txt') as f:
    #     timestamp = int(f.read())
    # print(timestamp)

    succFile, failFile, existFile, updateFile = [], [], [], []
    firstTimestamp = get_url_list1(timestamp)
    #print('222')
    #print(gmProds, smProds, qtProds, gmEx, succFile, failFile, existFile, updateFile)
    #print('222')
    # 发送邮件
    #mail(gmProds, smProds, qtProds, gmEx, succFile, failFile, existFile, updateFile)

    # 更新时间
    if any([succFile, failFile, existFile, updateFile]):
        print("Writing latest date time {} to".format(firstTimestamp), UPDATE_LOG_PATH_YYBG)
        firstTimestamp_str = firstTimestamp.strftime("%Y-%m-%d %H:%M:%S")
        with io.BytesIO(firstTimestamp_str.encode("utf-8")) as bio:
            ftp.storbinary(f"STOR {UPDATE_LOG_PATH_YYBG}", bio)

    # if any([succFile, failFile, existFile, updateFile]):
    #     with open(r'E:\脚本\ABN每周跑批入库\lastTime_yybg.txt', 'w') as f:
    #         f.write(str(firstTimestamp))
