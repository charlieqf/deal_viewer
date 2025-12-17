# -*- coding: utf-8 -*-
"""
cn-abs.com 证券信息爬取脚本

功能：
1. 登录 cn-abs.com
2. 搜索产品名称
3. 进入产品页面
4. 获取"产品证券"栏目下的证券信息

抽取字段：
- 证券名称
- 证券代码
- 发行量
- 还本方式
- 类型
- 发行利率
- 当前利率
- 预计到期日
- 大公国际（原始）
- 大公国际（当前）
"""

import requests
import json
import time
import random
from urllib.parse import quote, urlencode
import re

# ==================== 配置 ====================
CNABS_USERNAME = "18085157187"
CNABS_PASSWORD = "Password01"
BASE_URL = "https://www.cn-abs.com"

# 请求头
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Origin": "https://www.cn-abs.com",
    "Referer": "https://www.cn-abs.com/",
}


def list_ftp_directory_with_retry(ftp, path, retries=6):
    for attempt in range(retries):
        try:
            return list_ftp_directory(ftp, path)
        except (
            ftplib.error_temp,
            ftplib.error_perm,
            ftplib.error_reply,
            BrokenPipeError,
            TimeoutError,
            ConnectionError,
            socket.timeout,
        ) as e:
            print(
                f"Error occurred while listing directory: {e}. Retrying {retries - attempt - 1} more times."
            )
            # 如果是超时错误，可能需要重新连接FTP
            if (
                isinstance(e, (TimeoutError, socket.timeout, ConnectionError))
                and attempt < retries - 1
            ):
                try:
                    print("Connection timed out, attempting to reconnect FTP...")
                    # 保存原始连接信息
                    host = ftp.host
                    port = ftp.port
                    user = (
                        ftp._user if hasattr(ftp, "_user") else FTP_USER
                    )  # 备用用户名
                    passwd = (
                        ftp._passwd if hasattr(ftp, "_passwd") else FTP_PASS
                    )  # 备用密码

                    # 重新连接
                    try:
                        ftp.close()
                    except:
                        pass  # 忽略关闭连接时的错误

                    ftp.connect(host, port, timeout=60)  # 增加超时时间
                    ftp.login(user, passwd)
                    print("FTP reconnection successful")
                except Exception as reconnect_error:
                    print(f"Failed to reconnect to FTP: {reconnect_error}")

            # 增加等待时间，避免立即重试导致再次超时
            backoff_time = 5 * (attempt + 1)  # 逐渐增加等待时间
            print(f"Waiting {backoff_time} seconds before retrying...")
            time.sleep(backoff_time)

    raise Exception("Failed to list directory after multiple attempts")


def list_ftp_directory(ftp, path):
    """List files and directories in the given FTP path."""
    print(f"Listing directory: {path}")

    try:
        # 切换目录
        ftp.cwd(path)
    except ftplib.error_perm as e:
        if "550" in str(e):
            print(f"Directory {path} does not exist.")
            return []
        else:
            raise

    raw_items = []

    try:
        # 设置更长的超时时间
        if hasattr(ftp, "sock") and ftp.sock:
            ftp.sock.settimeout(120)  # 2分钟

        # 获取目录列表
        ftp.retrlines("NLST", raw_items.append)
    except ftplib.error_perm as e:
        print(f"NLST command failed: {e}, trying LIST instead")
        try:
            # 如果NLST指令失败，尝试LIST
            temp_items = []
            ftp.retrlines("LIST", temp_items.append)
            # 从LIST结果中提取文件名（最后一列）
            raw_items = [item.split()[-1] for item in temp_items if item.strip()]
        except Exception as list_error:
            print(f"LIST command also failed: {list_error}")
    except Exception as e:
        print(f"Error listing directory: {e}")
    finally:
        # 恢复原始超时设置
        if hasattr(ftp, "sock") and ftp.sock:
            ftp.sock.settimeout(None)

    # 过滤空项并返回
    items = [item for item in raw_items if item.strip()]

    print(f"Found {len(items)} items in {path}")
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
ftp.set_pasv(True)
# ftp.set_debuglevel(2)
# ftp.cwd(FTP_HOME_DIR)


# Connect to FTP server 192.168.1.211
ftp2 = ftplib.FTP()
ftp2.connect(FTP2_HOST, FTP2_PORT, timeout=600)
ftp2.login(FTP2_USER, FTP2_PASS)
ftp2.set_pasv(True)
# ftp2.set_debuglevel(2)
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

    # First try without using SmartProxy
    print("Attempting to download without proxy first...")
    try:
        response = requests.get(encoded_url, headers=headers, timeout=(10, 30))

        # Check if request was successful
        if (
            response.status_code == 200
            and response.headers.get("Content-Type") == "application/pdf"
        ):
            print("Download successful without proxy")
            return True, response.content

        # Check if this is an IP-related failure
        ip_related_failure = False
        if response.status_code in [403, 429, 451]:
            ip_related_failure = True
        elif response.status_code != 200:
            # Check response text for IP blocking messages
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
                    break

        if not ip_related_failure:
            # Failed but not due to IP issues
            print(
                f"Failed to download without proxy. Status: {response.status_code}. Not IP related."
            )
            return False, response.text
    except Exception as e:
        print(f"Error during non-proxy download attempt: {e}")
        # For connection errors, assume it could be IP related
        if isinstance(
            e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
        ):
            print("Connection error - might be IP related")
        else:
            # For other types of errors, return the error
            return False, str(e)

    # If we got here, either there was an IP-related failure or an exception occurred
    # Try again with SmartProxy (only if proxy is configured)
    if USE_PROXY and proxies is not None:
        print("Trying download with SmartProxy...")
        try:
            response = requests.get(
                encoded_url, headers=headers, proxies=proxies, timeout=(10, 30)
            )

            # Check if the content is actually a PDF
            print("Checking content from proxy request")
            if response.headers.get("Content-Type") == "application/pdf":
                print("Successfully downloaded with proxy")
                return True, response.content
            else:
                print(f"Failed to download with proxy. Status: {response.status_code}")
                return False, response.text
        except Exception as e:
            print(f"Error during proxy download attempt: {e}")
            return False, str(e)
    else:
        print("IP restriction detected but no proxy configured. Cannot download file.")
        return False, "No proxy available for IP-restricted download"


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

# # Smartproxy credentials and proxy URL
# username = "splci64blr"
# password = "6j0z1hrwFM4LbdheZ_"
# proxy_url = f"http://{username}:{password}@gate.smartproxy.com:10001"

# 代理配置
# ProxyJet返回423 Locked错误（账户锁定/限额/服务不可用）
# 由于脚本在中国境内运行，且直接访问可以获取token，尝试不使用代理

# 选项1：不使用代理（当前遇到IP限制，需要使用代理）
# USE_PROXY = False
# proxies = None
# proxy_url = "None (Direct Connection)"

# 选项2：使用ProxyJet代理（已启用以绕过IP限制）
USE_PROXY = True
proxy_string = "in.proxy-jet.io:1010:2506034iYZQ-resi_region-CN_Guangdong_Guangzhou-ip-7193938:rUGciFpmX7CwT12"
# 备用北京IP：
# proxy_string = "in.proxy-jet.io:1010:2506034iYZQ-resi_region-CN_Beijing_Jinrongjie-ip-1059836:rUGciFpmX7CwT12"

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
static_url = "https://www.chinamoney.com.cn/dqs/cm-s-notice-query/fileDownLoad.do?mode=open&contentId={}&priority=0"


def get_headers(url, use="pc"):
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
        "Mozilla/5.0 (X11; Linux x86_64; rv:76.0) Gecko/20100101 Firefox/76.0",
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
        "Mozilla/4.0 (compatible; MSIE 6.0; ) Opera/UCWEB7.0.2.37/28/999",
    ]
    """user_agent部分来源:https://blog.csdn.net/IT__LS/java/article/details/78880903"""
    referer = lambda url: re.search(
        "^((https://)|(https://))?([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}(/)",
        url,
    ).group()
    """正则来源:https://www.cnblogs.com/blacksonny/p/6055357.html"""
    if use == "phone":  # 随机选择一个
        agent = random.choice(phone_agent)
    else:
        agent = random.choice(pc_agent)

    headers = {
        "User-Agent": agent,
        "Referer": "https://www.chinamoney.com.cn/chinese/qwjsn/",
        "Cookie": "_ulta_id.CM-Prod.e9dc=9f8733fe96d3a061; AlteonP10=CLDtOSw/F6xDU59z7jzUGg$$; apache=4a63b086221745dd13be58c2f7de0338; ags=2a1ba4d47b619c011c19c1cc4b3c0c32; lss=fd9e664ef34511dcdc4a51a4e8d84abc; _ulta_ses.CM-Prod.e9dc=1187f9d14693de99; isLogin=0",
    }
    return headers


def get_proxy():
    return requests.get("http://162.14.106.158:5010/get/?type=http").json()


def delete_proxy(proxy):
    requests.get("http://162.14.106.158:5010/delete/?proxy={}".format(proxy))


def get_sign_and_info_level():
    baseurl = "https://www.chinamoney.com.cn/lss/rest/cm-s-account/getLT"
    params = {"type": "0"}

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Content-Length": "0",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.chinamoney.com.cn/chinese/qwjsn/",
        "Cookie": "_ulta_id.CM-Prod.e9dc=9f8733fe96d3a061; AlteonP10=CLDtOSw/F6xDU59z7jzUGg$$; apache=4a63b086221745dd13be58c2f7de0338; ags=2a1ba4d47b619c011c19c1cc4b3c0c32; lss=fd9e664ef34511dcdc4a51a4e8d84abc; _ulta_ses.CM-Prod.e9dc=1187f9d14693de99; isLogin=0",
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
        # First try without using proxy to save costs
        print("Attempting to get sign and info_level without proxy...")
        response = requests.post(baseurl, data=params, headers=headers)
        print(f"Response status: {response.status_code}")

        # Check if this is an IP-related failure
        ip_related_failure = False
        if response.status_code in [403, 429, 451]:
            ip_related_failure = True
            print(f"Received {response.status_code} error - likely IP restriction")
        elif response.status_code != 200:
            try:
                response_text = response.text.lower()
                ip_block_indicators = [
                    "blocked",
                    "forbidden",
                    "access denied",
                    "地址被禁止",
                    "访问受限",
                    "访问被拒绝",
                ]
                for indicator in ip_block_indicators:
                    if indicator.lower() in response_text:
                        ip_related_failure = True
                        print(f"Detected IP blocking indicator: {indicator}")
                        break
            except:
                pass

        # If IP-related failure, retry with proxy (only if proxy is configured)
        if ip_related_failure:
            if USE_PROXY and proxies is not None:
                print("Retrying with SmartProxy due to IP restriction...")
                response = requests.post(
                    baseurl, data=params, headers=headers, proxies=proxies
                )
                print(f"Response status with proxy: {response.status_code}")
            else:
                print("IP restriction detected but no proxy configured. Cannot retry.")
                print(f"Response text (first 500 chars): {response.text[:500]}")
                raise Exception(
                    f"IP restriction detected (HTTP {response.status_code}) and no proxy available"
                )

        # 使用代理访问
        requestsdata = response.json()
        data = requestsdata["data"]
        print("data =", data)

        info_level = data["UT"].replace("\n", "")
        sign = data["sign"].replace("\n", "")

        print("sign =", sign)
        print("info_level =", info_level)

        return sign, info_level

    except Exception as e:
        print("error getting sign and info_level:", e)
        return None, None


# 获取Sign和InfoLevel
def get_token():
    baseurl = "https://www.chinamoney.com.cn/lss/rest/cm-s-account/getLT"
    params = {"type": "0"}

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
    headers = get_headers("https://www.chinamoney.com.cn/chinese/qwjsn/?")

    try:
        # First try without using proxy to save costs
        print("Attempting to get token without proxy...")
        response = requests.post(baseurl, data=params, headers=headers)
        print(f"Response status: {response.status_code}")

        # Check if this is an IP-related failure
        ip_related_failure = False
        if response.status_code in [403, 429, 451]:
            ip_related_failure = True
            print(f"Received {response.status_code} error - likely IP restriction")
        elif response.status_code != 200:
            try:
                response_text = response.text.lower()
                ip_block_indicators = [
                    "blocked",
                    "forbidden",
                    "access denied",
                    "地址被禁止",
                    "访问受限",
                    "访问被拒绝",
                ]
                for indicator in ip_block_indicators:
                    if indicator.lower() in response_text:
                        ip_related_failure = True
                        print(f"Detected IP blocking indicator: {indicator}")
                        break
            except:
                pass

        # If IP-related failure, retry with proxy (only if proxy is configured)
        if ip_related_failure:
            if USE_PROXY and proxies is not None:
                print("Retrying with SmartProxy due to IP restriction...")
                response = requests.post(
                    baseurl, data=params, headers=headers, proxies=proxies
                )
                print(f"Response status with proxy: {response.status_code}")
            else:
                print("IP restriction detected but no proxy configured. Cannot retry.")
                print(f"Response text (first 500 chars): {response.text[:500]}")
                raise Exception(
                    f"IP restriction detected (HTTP {response.status_code}) and no proxy available"
                )

        # 使用代理访问
        requestsdata = response.json()
        data = requestsdata["data"]
        print("data =", data)
        # 打印响应内容
        return data
    except Exception as e:
        print(f"Error getting token: {e}")
        return None


def get_html(timestamp):
    # Only test proxy if it's configured
    if USE_PROXY and proxies is not None:
        test_url = "https://ip.smartproxy.com/json"
        try:
            response = requests.get(test_url, proxies=proxies)
            print(response.status_code, response.json())
        except Exception as e:
            print(f"Proxy test failed: {e}")
    else:
        print("Proxy not configured, skipping proxy test...")

    # 动态获取token，使用备用方案提高容错性
    print("获取新的token...")
    token = get_token()
    if token is None:
        print("警告: 无法获取token，尝试使用备用方法...")
        sign, info_level = get_sign_and_info_level()
        if sign is None or info_level is None:
            raise Exception("无法获取有效的token和sign，请检查网络连接和代理设置")
    else:
        print("token is " + str(token))
        info_level = token["UT"].replace("\n", "")
        sign = token["sign"].replace("\n", "")
    
    print("info_level:", info_level[:50] + "...")
    print("sign:", sign[:50] + "...")

    # 创建Session对象来自动管理Cookie
    session = requests.Session()

    # 先访问主页获取有效的Cookie
    print("访问主页获取Cookie...")
    try:
        home_url = "https://www.chinamoney.com.cn/chinese/qwjsn/"
        home_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Host": "www.chinamoney.com.cn",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }
        home_response = session.get(home_url, headers=home_headers, verify=False, timeout=10)
        print(f"主页访问状态: {home_response.status_code}")
        print(f"获取到的Cookie: {session.cookies.get_dict()}")

        # 如果没有自动获取到Cookie，手动设置
        if not session.cookies.get_dict():
            print("未自动获取到Cookie，手动设置浏览器Cookie...")
            session.cookies.set("AlteonP10", "AYasNCw/F6zhU0YxOaADcg$$", domain="www.chinamoney.com.cn")
            session.cookies.set("lss", "953d30744145d363215192a47c98ceb5", domain="www.chinamoney.com.cn")
            session.cookies.set("isLogin", "0", domain="www.chinamoney.com.cn")
            print(f"手动设置后的Cookie: {session.cookies.get_dict()}")
    except Exception as e:
        print(f"访问主页失败: {e}")
        print("继续尝试API请求...")

    baseurl = "https://www.chinamoney.com.cn/ses/rest/cm-u-notice-ses-cn/query"

    # ==================== 日期范围配置 ====================
    # 使用日期范围查询，不依赖游标分页
    USE_DATE_RANGE = False  # 改为False使用时间戳+游标分页
    START_DATE = "2025-08-01"  # 起始日期（USE_DATE_RANGE=True时生效）
    END_DATE = "2025-10-01"    # 结束日期（USE_DATE_RANGE=True时生效）
    # ====================================================
    
    params = {
        "sort": "date",
        "text": "上市流通 ABN",
        "date": "customize" if USE_DATE_RANGE else "all",  # 自定义日期范围
        "field": "title",
        "start": START_DATE if USE_DATE_RANGE else "",
        "end": END_DATE if USE_DATE_RANGE else "",
        "pageIndex": "1",
        "pageSize": "15",
        "public": "false",
        "infoLevel": info_level,
        "sign": sign,
        "channelIdStr": "2496,2556,2632,2663,2589,2850,3300,",  # 去掉空格
        "nodeLevel": "1",
        "op": "top",  # 第一页用top，后续页用next
        "searchAfter": "",  # 游标分页参数
    }
    
    if USE_DATE_RANGE:
        print(f"使用日期范围查询: {START_DATE} 到 {END_DATE}")

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

    # headers = get_headers('https://www.chinamoney.com.cn/chinese/qwjsn/?')
    # Define headers for the request (移除固定Cookie，让Session自动管理)
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "0",
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
        "sec-ch-ua-platform": '"Windows"',
    }
    requests.packages.urllib3.disable_warnings()
    shortnameList = []
    urlList = []
    
    # 不再从 abn_urls.txt 加载旧URL，每次都是全新获取
    # abn_urls.txt 仅用于保存本次获取的URL，方便调试
    print("开始获取新的URL列表...")

    # 初始化为当前timestamp，后续会更新为实际最新时间
    latestUpdateTime = parse(timestamp)
    has_new_data = False  # 标记是否有新数据

    # 增加翻页数量以支持长时间未运行的情况
    # 测试阶段：10页 × 15条 = 150条数据
    # 正式运行：100页 × 15条 = 1500条数据
    MAX_PAGES = 100  # 抓取前100页
    search_after = ""  # 游标分页参数
    
    for pageIndex in range(1, MAX_PAGES + 1):
        print(f"\n{'='*60}")
        print(f"正在获取第 {pageIndex} 页数据...")
        print(f"{'='*60}")
        
        # 更新分页参数
        params["pageIndex"] = str(pageIndex)
        params["op"] = "top" if pageIndex == 1 else "next"
        params["searchAfter"] = search_after
        
        if USE_DATE_RANGE:
            print(f"[分页参数] pageIndex={pageIndex}, op={params['op']}, date={START_DATE}~{END_DATE}, searchAfter='{search_after[:50] if search_after else ''}'")
        else:
            print(f"[分页参数] pageIndex={pageIndex}, op={params['op']}, searchAfter='{search_after[:50] if search_after else ''}'")

        # First try without using proxy to save costs
        # 特殊的POST请求：参数在URL中（params），但使用POST方法，请求体为空（Content-Length: 0）
        print(f"Attempting to post to {baseurl} without proxy...")
        r = session.post(baseurl, params=params, headers=headers, verify=False)

        status_code = r.status_code
        print(f"HTTP Status Code: {status_code}")

        # Check if this is an IP-related failure (403, 429, 451)
        ip_related_failure = False
        if status_code in [403, 429, 451]:
            ip_related_failure = True
            print(f"Received {status_code} error - likely IP restriction")
        elif status_code != 200:
            # Check response text for IP blocking indicators
            try:
                response_text = r.text.lower()
                ip_block_indicators = [
                    "blocked",
                    "forbidden",
                    "access denied",
                    "地址被禁止",
                    "访问受限",
                    "访问被拒绝",
                ]
                for indicator in ip_block_indicators:
                    if indicator.lower() in response_text:
                        ip_related_failure = True
                        print(f"Detected IP blocking indicator: {indicator}")
                        break
            except:
                pass

        # If IP-related failure, retry with proxy (only if proxy is configured)
        if ip_related_failure:
            if USE_PROXY and proxies is not None:
                print("Retrying with SmartProxy due to IP restriction...")
                try:
                    r = session.post(
                        baseurl,
                        params=params,
                        headers=headers,
                        proxies=proxies,
                        verify=False,
                    )
                    status_code = r.status_code
                    print(f"HTTP Status Code with proxy: {status_code}")
                except Exception as e:
                    print(f"Error using proxy: {e}")
                    raise
            else:
                print("IP restriction detected but no proxy configured. Cannot retry.")
                print(f"Response text (first 500 chars): {r.text[:500]}")
                raise Exception(
                    f"IP restriction detected (HTTP {status_code}) and no proxy available"
                )

        # Check if we got a valid response
        if status_code != 200:
            print(f"Failed with status code: {status_code}")
            print(f"Response text (first 500 chars): {r.text[:500]}")
            raise Exception(f"Failed to fetch data: HTTP {status_code}")

        # Try to parse JSON response
        try:
            requestsdata = r.json()
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response. Status: {status_code}")
            print(f"Response text (first 1000 chars): {r.text[:1000]}")
            raise Exception(f"Invalid JSON response from server: {e}")
        
        # 详细打印响应结构
        print(f"Response keys: {requestsdata.keys()}")
        if "data" not in requestsdata:
            print(f"❌ 响应中没有'data'字段！")
            print(f"完整响应: {requestsdata}")
            raise Exception("API响应格式错误：缺少'data'字段")
        
        data = requestsdata["data"]
        print(f"Data keys: {data.keys()}")
        
        if "result" not in data:
            print(f"❌ data中没有'result'字段！")
            print(f"完整data: {data}")
            raise Exception("API响应格式错误：缺少'result'字段")
        
        result = data["result"]
        print(f"Result keys: {result.keys()}")
        
        if "pageItems" not in result:
            print(f"❌ result中没有'pageItems'字段！")
            print(f"完整result: {result}")
            raise Exception("API响应格式错误：缺少'pageItems'字段")
        
        pageItems = result["pageItems"]
        
        # 提取searchAfter用于下一页（游标分页）
        # searchAfter来自响应的sortNext字段
        search_after_found = False

        # 从result中的sortNext字段获取
        if "sortNext" in result:
            sort_next = result["sortNext"]
            # sortNext是一个数组：[timestamp, score, id]
            if isinstance(sort_next, list) and len(sort_next) == 3:
                # 转换为逗号分隔的字符串
                search_after = ",".join(str(x) for x in sort_next)
                print(f"[分页] 从sortNext获取到searchAfter: '{search_after}'")
                print(f"  sortNext数组: {sort_next}")
                search_after_found = True

        if not search_after_found:
            print(f"[分页] 未找到sortNext字段，保持searchAfter为空")
            print(f"[调试] result中的字段: {result.keys()}")

        if not pageItems:
            print("⚠️ 当前页没有数据（pageItems为空列表）")
            print(f"完整result: {result}")
            break

        print(f"当前页有 {len(pageItems)} 条数据")
        page_new_count = 0  # 当前页新数据计数
        
        for i in pageItems:
            release_date_ts = int(i["releaseDate"]) / 1000
            release_date = datetime.fromtimestamp(release_date_ts)

            # 始终更新latestUpdateTime为遇到的最新时间（无论是否处理）
            if release_date > latestUpdateTime:
                latestUpdateTime = release_date

            # 日期范围模式：收集所有数据；时间戳模式：只处理比上次更新时间更新的数据
            should_process = USE_DATE_RANGE or (release_date > parse(timestamp))
            
            if should_process:
                has_new_data = True
                page_new_count += 1
                title = (
                    i["title"][9:-1]
                    .replace("<font color='red'>", "")
                    .replace("</font>", "")
                )
                short_name_match = re.findall("\d{2}.+?\d{3}", title)
                if short_name_match:
                    short_name = short_name_match[0]
                    if short_name not in shortnameList:
                        shortnameList.append(short_name)
                        urlList.append(
                            short_name
                            + ":"
                            + i["dealPath"]
                            + ":"
                            + release_date.strftime("%Y-%m-%d")
                        )
                        print(f"  ✓ 新产品: {short_name} (日期: {release_date.strftime('%Y-%m-%d')})")
                    else:
                        print(f"  ⊗ 跳过重复: {short_name}")
                else:
                    print(f"  ⚠️ Warning: Could not extract short_name from title: {title}")
            # 不要break，继续处理当前页的其他项目
        
        print(f"第 {pageIndex} 页: 共 {len(pageItems)} 条数据，其中新数据 {page_new_count} 条")

        # 如果当前页没有新数据，且已经翻了至少5页，可以停止翻页
        # 这样可以避免因为第1页恰好都是旧数据而过早停止
        if not has_new_data and pageIndex >= 5:
            print(f"No new data found on page {pageIndex}, stopping pagination")
            break
        
        has_new_data = False  # 重置标记，检查下一页
        
        # sleep for a while (增加延迟避免请求过快)
        if pageIndex < MAX_PAGES:  # 不是最后一页才延迟
            delay = random.uniform(2, 4)
            print(f"等待 {delay:.1f} 秒后继续...")
            time.sleep(delay)

    print(f"\n{'='*80}")
    print(f"数据获取完成！")
    print(f"{'='*80}")
    print(f"找到的URL总数: {len(urlList)}")
    print(f"上次更新时间: {timestamp}")
    print(f"本次最新时间: {latestUpdateTime}")
    print(f"时间差: {(latestUpdateTime - parse(timestamp)).days} 天")
    print(f"{'='*80}\n")
    return latestUpdateTime, urlList


# post_list_data() 已删除 - 使用代理池的旧实现，已被智能代理策略替代


# 目前只抽取募集说明书与资产运营报告
def get_usefulPDF(pdf_list):
    q1 = []
    q2 = []
    exist = []
    p = re.compile("第.+?期")
    done = 0
    for i in pdf_list:
        if "募集说明书" in i[1] and done == 0:
            done = 1
            q1.append(i)
        if "发行" in i[1]:
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

    # 获取动态token，使用备用方案提高容错性
    token = get_token()
    if token is None:
        print("警告: 无法获取token，尝试使用备用方法...")
        sign, info_level = get_sign_and_info_level()
        if sign is None or info_level is None:
            print("Failed to get token for file list")
            return []
    else:
        info_level = token["UT"].replace("\n", "")
        sign = token["sign"].replace("\n", "")

    url = "https://www.chinamoney.com.cn/ses/rest/cm-u-notice-ses-cn/query"
    data = {
        "sort": "date",
        "text": product_name,
        "date": "all",
        "field": "title",
        "start": "",
        "end": "",
        "pageIndex": "1",
        "pageSize": "15",
        "public": "false",
        "infoLevel": info_level,
        "sign": sign,
        "channelIdStr": "2496, 2556, 2632, 2663, 2589, 2850, 3300,",
        "nodeLevel": "1",
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
        "sec-ch-ua-platform": '"Windows"',
    }

    # First try without using proxy to save costs
    print("Attempting to fetch file list without proxy...")
    rs = requests.post(url, data=data, headers=headers)
    time.sleep(4.33)
    print("rs.status =", rs.status_code)

    # Check if this is an IP-related failure
    ip_related_failure = False
    if rs.status_code in [403, 429, 451]:
        ip_related_failure = True
        print(f"Received {rs.status_code} error - likely IP restriction")
    elif rs.status_code != 200:
        try:
            response_text = rs.text.lower()
            ip_block_indicators = [
                "blocked",
                "forbidden",
                "access denied",
                "地址被禁止",
                "访问受限",
                "访问被拒绝",
            ]
            for indicator in ip_block_indicators:
                if indicator.lower() in response_text:
                    ip_related_failure = True
                    print(f"Detected IP blocking indicator: {indicator}")
                    break
        except:
            pass

    # If IP-related failure, retry with proxy (only if proxy is configured)
    if ip_related_failure:
        if USE_PROXY and proxies is not None:
            print("Retrying with SmartProxy due to IP restriction...")
            rs = requests.post(url, data=data, headers=headers, proxies=proxies)
            time.sleep(4.33)
            print("rs.status with proxy =", rs.status_code)
        else:
            print("IP restriction detected but no proxy configured. Cannot retry.")
            print(f"Response text (first 500 chars): {rs.text[:500]}")
            raise Exception(
                f"IP restriction detected (HTTP {rs.status_code}) and no proxy available"
            )

    # Check for errors
    if rs.status_code != 200:
        print(f"Error: HTTP {rs.status_code}")
        print(f"Response text (first 500 chars): {rs.text[:500]}")
        raise Exception(f"Failed to fetch file list: HTTP {rs.status_code}")

    try:
        rs = json.loads(rs.text)
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON response")
        print(f"Response text (first 1000 chars): {rs.text[:1000]}")
        raise Exception(f"Invalid JSON response: {e}")

    total = rs["data"]["result"]["total"]

    if total > 50:
        time.sleep(random.uniform(1.5, 2.9))
        data["pageSize"] = total

        # First try without proxy
        print(f"Fetching full list ({total} items) without proxy...")
        rs = requests.post(url, data=data, headers=headers)
        time.sleep(3.156)

        # Check if this is an IP-related failure
        ip_related_failure = False
        if rs.status_code in [403, 429, 451]:
            ip_related_failure = True
            print(
                f"Received {rs.status_code} error on second request - likely IP restriction"
            )
        elif rs.status_code != 200:
            try:
                response_text = rs.text.lower()
                ip_block_indicators = [
                    "blocked",
                    "forbidden",
                    "access denied",
                    "地址被禁止",
                    "访问受限",
                    "访问被拒绝",
                ]
                for indicator in ip_block_indicators:
                    if indicator.lower() in response_text:
                        ip_related_failure = True
                        print(
                            f"Detected IP blocking indicator on second request: {indicator}"
                        )
                        break
            except:
                pass

        # If IP-related failure, retry with proxy (only if proxy is configured)
        if ip_related_failure:
            if USE_PROXY and proxies is not None:
                print(
                    "Retrying second request with SmartProxy due to IP restriction..."
                )
                rs = requests.post(url, data=data, headers=headers, proxies=proxies)
                time.sleep(3.156)
                print("rs.status with proxy (second request) =", rs.status_code)
            else:
                print(
                    "IP restriction detected on second request but no proxy configured. Cannot retry."
                )
                print(f"Response text (first 500 chars): {rs.text[:500]}")
                raise Exception(
                    f"IP restriction detected (HTTP {rs.status_code}) and no proxy available"
                )

        # Check for errors
        if rs.status_code != 200:
            print(f"Error on second request: HTTP {rs.status_code}")
            print(f"Response text (first 500 chars): {rs.text[:500]}")
            raise Exception(f"Failed to fetch full file list: HTTP {rs.status_code}")

        try:
            rs = json.loads(rs.text)
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response on second request")
            print(f"Response text (first 1000 chars): {rs.text[:1000]}")
            raise Exception(f"Invalid JSON response: {e}")

    pdf_list = []
    pageItems = rs["data"]["result"]["pageItems"]
    for i in pageItems:
        if len(i["paths"]) == 1:
            z = re.search("\d{4}/?\d{2}/?\d{2}", i["paths"][0]).group().replace("/", "")
            date = z[:4] + "-" + z[4:6] + "-" + z[6:8] + " 00:00:00.000"
            pdf_list.append(
                [
                    i["id"],
                    i["title"].replace("<font color='red'>", "").replace("</font>", ""),
                    date,
                ]
            )
        else:
            print(len(i["paths"]), i["id"], i["title"], "XXX")
    return pdf_list


# get_url_list() 已删除 - 旧版实现，已被 get_html() 替代


# upload_dir() 已删除 - 旧版FTP上传，只被已删除的 upload_211_bak() 使用


# upload_folder_to_ftp() 已删除 - 有bug且从未被调用


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
        if "." in ftp1_item_path:
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

def upload_211(trustcode):
    month = str(datetime.now().month)
    folder = create_dir_on_ftp(ftp2, "/增量文档", month)
    source_folder = create_dir_on_ftp(ftp2, folder, trustcode)

    target_folder = create_dir_on_ftp(ftp2, "/DealViewer/TrustAssociatedDoc", trustcode)

    upload_ftp_folder_to_ftp(ftp, ftp2, source_folder, target_folder)


# upload_211_bak() 已删除 - 备份函数，使用旧的网络共享路径


def crawl_pdf(product_name, i, type_f):
    pdf_file_name = i[1] + ".pdf"

    cache_pdf_path = os.path.join(cache_folder, pdf_file_name)
    file_exists = os.path.exists(cache_pdf_path)

    month = str(datetime.now().month)

    if not file_exists:
        url = static_url.format(int(i[0]))

        # 使用智能代理策略下载PDF
        try:
            print("downloading file from url:", url)
            success, content = get_web_pdf_content_with_retry(url)
            if success:
                with open(cache_pdf_path, "wb") as f:
                    f.write(content)
                file_exists = True
                print(f"✓ PDF下载成功: {pdf_file_name}")
            else:
                print(f"✗ PDF下载失败: {pdf_file_name}, 原因: {content}")
        except Exception as e:
            print(f"✗ PDF下载异常: {pdf_file_name}, 错误: {e}")
            time.sleep(random.uniform(1, 5))

    if file_exists:
        ftp_folder = create_dir_on_ftp(ftp, "/Products/资产支持票据", product_name[2])
        ftp_file_path = os.path.join(ftp_folder, pdf_file_name)

        upload_file_to_ftp(
            ftp, cache_pdf_path, ftp_folder, ftp_file_path, pdf_file_name
        )

        if type_f == 1:
            create_dir_on_ftp(ftp, "/增量文档", month)
            folder = create_dir_on_ftp(ftp, f"/增量文档/{month}", product_name[1])
            ftp_folder = create_dir_on_ftp(ftp, folder, "ProductReleaseInstructions")
            ftp_file_path = os.path.join(ftp_folder, pdf_file_name)
            upload_file_to_ftp(
                ftp, cache_pdf_path, ftp_folder, ftp_file_path, pdf_file_name
            )

        if type_f == 2:
            create_dir_on_ftp(ftp, "/增量文档", month)
            folder = create_dir_on_ftp(ftp, f"/增量文档/{month}", product_name[1])
            ftp_folder = create_dir_on_ftp(ftp, folder, "TrusteeReport")
            ftp_file_path = os.path.join(ftp_folder, pdf_file_name)
            upload_file_to_ftp(
                ftp, cache_pdf_path, ftp_folder, ftp_file_path, pdf_file_name
            )

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


# crawl_pdf_bak() 已删除 - 备份函数，使用旧的网络共享路径


def upload_114(product_name, q1, q2):
    print(product_name)
    month = str(datetime.now().month)
    if any([q1, q2]):
        print("creating folder for", product_name[2])
        create_dir_on_ftp(ftp, "/Products/资产支持票据", product_name[2])
        folder = create_dir_on_ftp(ftp, "/增量文档", month)
        create_dir_on_ftp(ftp, folder, product_name[1])

        # if not os.path.exists(r'\\172.16.7.114\/Products/资产支持票据\{}'.format(product_name[2])):
        #     os.mkdir(r'\\172.16.7.114\/Products/资产支持票据\{}'.format(product_name[2]))
        # if not os.path.exists(r'\\172.16.7.114\/增量文档\{}\{}'.format(str(datetime.now().month), product_name[1])):
        #     os.mkdir(r'\\172.16.7.114\/增量文档\{}\{}'.format(str(datetime.now().month), product_name[1]))

    if q1:
        create_dir_on_ftp(
            ftp, f"/增量文档/{month}/{product_name[1]}", "ProductReleaseInstructions"
        )
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
    print("newProd process", url)
    df = pd.DataFrame()
    url = "https://www.chinamoney.com.cn" + url

    # 检查url_done.txt文件是否存在
    url_done_file = "url_done.txt"
    if os.path.exists(url_done_file):
        with open(url_done_file, "r", encoding="utf8") as f:
            done_urls = f.read().splitlines()
            if url in done_urls:
                print("文件判断此url已处理完成")
                return
    else:
        print("url_done.txt not found, creating new file")
        done_urls = []

    flag = 0

    retries = 5

    while flag == 0 and retries > 0:
        try:
            retries -= 1
            wd = pd.read_html(url)
            flag = 1
        except:
            print("pd.read_html failed")
            time.sleep(random.uniform(1, 5))

    df = pd.concat(wd, ignore_index=True)
    fullName = df.iloc[1][1]
    fullName = fullName.split("票据")[0] + "票据"
    shortName = df.iloc[1][3]
    shortName = (
        shortName[2 : shortName.find("ABN") + 3]
        + "20"
        + shortName[:2]
        + "-"
        + str(int(shortName[shortName.find("ABN") + 3 : shortName.find("ABN") + 6]))
    )
    offer_type = df.iloc[-1][3]

    # ❌ 移除测试用的产品过滤，处理所有产品
    # if "一汽" not in fullName:
    #     return

    # 公募：PublicOffering，私募：PrivateEquity，其它：Others
    if offer_type == "公开发行":
        CollectionMethod = "PublicOffering"
    elif offer_type == "定向发行":
        CollectionMethod = "PrivateEquity"
    else:
        CollectionMethod = "Others"

    # # conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
    # #                        database='PortfolioManagement', charset='utf8')
    cursor = conn.cursor()

    TrustStatus = "Duration"
    TrustName = fullName
    TrustNameShort = shortName
    TrustCode = "".join([i[0].upper() + i[1:] for i in lazy_pinyin(TrustNameShort)])

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

    # return

    isExist = "select 1 from TrustManagement.Trust where TrustName=N'{}' or TrustCode='{}' or TrustNameShort=N'{}'".format(
        TrustName, TrustCode, TrustNameShort
    )

    cursor.execute(isExist)
    res = cursor.fetchone()
    if res:
        print(TrustName, "数据库已存在")

        with open("核查冲突产品.txt", "a", encoding="utf8") as f:
            f.write(df.iloc[1][3] + " ")
            f.write(TrustName + " ")
            f.write(TrustCode + " ")
            f.write(TrustNameShort + "\n")
        # return
    else:
        selectTrustId = (
            "select max(TrustId)+1 from TrustManagement.Trust where TrustId<50000"
        )
        cursor.execute(selectTrustId)
        TrustId = cursor.fetchone()[0]

        # 插入Trust数据
        insertTrust = """
            SET IDENTITY_INSERT TrustManagement.Trust ON ;
            insert into TrustManagement.Trust(TrustId,TrustCode,TrustName,TrustNameShort,IsMarketProduct,TrustStatus) values({},'{}',N'{}',N'{}',1,'Duration');
            SET IDENTITY_INSERT TrustManagement.Trust OFF ;
        """.format(
            TrustId, TrustCode, TrustName, TrustNameShort
        )
        print(insertTrust)

        try:
            cursor.execute(insertTrust)
            conn.commit()
            print(TrustName, "基础表信息插入完成!")
        except Exception as e:
            print("err1", e)
            # print(TrustName)
            # print(insertTrust)
            return

        # 插入同步表数据
        insertFATrust = """
            SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust ON ;
            insert into FixedIncomeSuite.Analysis.Trust(TrustId,TrustCode,TrustName) values({},'{}',N'{}');
            SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust OFF ;
        """.format(
            TrustId, TrustCode, TrustName
        )
        print(insertFATrust)

        try:
            cursor.execute(insertFATrust)
            conn.commit()
            print(TrustName, "同步表信息插入完成!")
        except Exception as e:
            print("errFA", e)
            # print(TrustName)
            # print(insertFATrust)

        # 插入TrustInfoExtension数据
        RegulatoryOrg = "NAFMII"
        MarketPlace = "InterBank"

        TrustInfoExtension = (
            "insert into TrustManagement.TrustInfoExtension(TrustId, StartDate, EndDate, ItemId, ItemCode, ItemValue) values "
            "({}, GETDATE(), null, null, 'MarketCategory','ABN'),"
            "({}, GETDATE(), null, null, 'RegulatoryOrg','{}'),"
            "({}, GETDATE(), null, null, 'MarketPlace', '{}'),"
            "({}, GETDATE(), null, null, 'AssetType',null),"
            "({}, GETDATE(), null, null, 'BasicAssetType',null),"
            "({}, GETDATE(), null, null, 'CollectionMethod', '{}');".format(
                TrustId,
                TrustId,
                RegulatoryOrg,
                TrustId,
                MarketPlace,
                TrustId,
                TrustId,
                TrustId,
                CollectionMethod,
            )
        )

        try:
            cursor.execute(TrustInfoExtension)
            cursor.execute(
                "update TrustManagement.TrustInfoExtension set ItemValue=null where ItemValue='nan'"
            )
            conn.commit()
            print(TrustId, TrustName, "TrustInfoExtension表数据插入完成!")
        except Exception as e:
            print("err2", e)
            print(TrustName)
            print(TrustInfoExtension)
            return

    sql = f"select TrustId from TrustManagement.Trust where TrustName = N'{TrustName}'"
    print(f"查询TrustId SQL: {sql}")
    cursor.execute(sql)
    result = cursor.fetchone()
    
    if result is None:
        print(f"❌ 错误: 无法在数据库中找到产品 '{TrustName}'")
        print(f"可能原因: 1) 产品名称包含特殊字符 2) 插入失败但未报错 3) 事务未提交")
        # 尝试再次查询确认
        cursor.execute("select top 5 TrustId, TrustName from TrustManagement.Trust order by TrustId desc")
        recent = cursor.fetchall()
        print(f"最近插入的5个产品:")
        for r in recent:
            print(f"  TrustId={r[0]}, TrustName={r[1]}")
        return None
    
    TrustId = result[0]
    print(f"✓ 找到产品 TrustId={TrustId}, TrustName={TrustName}")

    if CollectionMethod == "PublicOffering":
        print(f"✓ 公开发行产品，开始处理文档...")
        product_name = [TrustId, TrustCode, TrustName]
        file_list = get_file_list(TrustName)
        q1, q2 = get_usefulPDF(file_list)
        print(f"产品发行文档(q1): {q1}")
        print(f"运营报告文档(q2): {q2}")
        upload_114(product_name, q1, q2)
        if q1:
            smsExist = 0
            upload_211(product_name[1])
            print(f"开始处理 {len(q1)} 个产品发行文档...")
            for idx, q in enumerate(q1, 1):
                print(f"  处理文档 {idx}/{len(q1)}: {q[1]}")
                insert_into_db(product_name, q, 1)
                if "说明书" in q[1]:
                    smsExist = 1
                    print(f"    ✓ 包含'说明书'关键词")
            if not smsExist:
                print(f"⚠️ 警告: 未找到说明书文档")
                gmEx.add(product_name[2])
        else:
            print(f"⚠️ 警告: 没有产品发行文档(q1为空)")
    else:
        print(f"⊗ 非公开发行产品，跳过文档处理")

    # 记录已处理的URL
    with open("url_done.txt", "a", encoding="utf8") as f:
        f.write(url + "\n")

    return offer_type, TrustName


def insert_into_db(product_name, q, typy_q):
    cursor = conn.cursor()
    # sql_i = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values(%s,%s,'NULL',%s,%s,'pdf',%s,'yixianfeng')".format()
    Trustid = product_name[0]
    TrustCode = product_name[1]
    if typy_q == 1:
        FileCategory = "ProductReleaseInstructions"
        FilePath = "DealViewer/TrustAssociatedDoc/{}/{}/".format(
            TrustCode, FileCategory
        )
    if typy_q == 2:
        FileCategory = "TrusteeReport"
        FilePath = "DealViewer/TrustAssociatedDoc/{}/{}/".format(
            TrustCode, FileCategory
        )

    file_name = q[1] + ".pdf"
    created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    sql_i = f"insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values({Trustid}, '{FileCategory}','NULL','{FilePath}',N'{file_name}','pdf','{created}','goldenstand')"

    print(sql_i)
    cursor.execute(sql_i)
    # cursor.execute(sql_i, (
    #     Trustid, FileCategory, FilePath, q[1] + '.pdf', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    print("asd")
    print(q)
    print(q[1])
    print("asd")
    if "募集说明书" in q[1]:
        # sql_i1 = "insert into PortfolioManagement.dbo.DisclosureOfInformation values(%s,%s,%s,%s,%s)".format(
        # )
        # # 之前记录披露时间，现在改成跟文档表一样的入库时间
        # cursor.execute(sql_i1,
        #                (Trustid, TrustCode, q[1], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), typy_q))

        sql_i1 = f"insert into PortfolioManagement.dbo.DisclosureOfInformation values({Trustid}, '{TrustCode}', N'{q[1]}', '{created}', {typy_q})"
        print(sql_i1)
        cursor.execute(sql_i1)

        # 查询TrustDocumentID
        sql = f"select TrustDocumentID from DV.TrustAssociatedDocument where filename=N'{q[1]}.pdf'"
        print(f"查询TrustDocumentID SQL: {sql}")
        cursor.execute(sql)
        result = cursor.fetchone()
        
        if result is None:
            print(f"❌ 错误: 无法找到文档记录，filename='{q[1]}.pdf'")
            print(f"可能原因: 1) 文档未插入成功 2) 文件名不匹配")
            # 查询最近插入的文档
            cursor.execute(f"select top 5 TrustDocumentID, FileName from DV.TrustAssociatedDocument where Trustid={Trustid} order by TrustDocumentID desc")
            recent_docs = cursor.fetchall()
            print(f"该产品最近插入的5个文档:")
            for doc in recent_docs:
                print(f"  TrustDocumentID={doc[0]}, FileName={doc[1]}")
            return  # 跳过ProductsStateInformation插入
        
        tdid = result[0]
        print(f"✓ 找到TrustDocumentID={tdid}")

        # 插入ProductsStateInformation
        sql_i2 = f"insert into TaskCollection.dbo.ProductsStateInformation(Trustid,TrustDocumentID,FileType,StateType) values({Trustid}, {tdid}, {typy_q}, 9)"
        print(f"插入ProductsStateInformation SQL: {sql_i2}")
        try:
            cursor.execute(sql_i2)
            print(f"✓ ProductsStateInformation插入成功: TrustDocumentID={tdid}")
        except Exception as e:
            print(f"❌ ProductsStateInformation插入失败: {e}")
            print(f"SQL: {sql_i2}")
            # 不抛出异常，继续处理

    conn.commit()
    # conn.close()
    print("single insert done")


# insert_db() 已删除 - 使用pymssql的旧版本，已被 insert_into_db() 替代

# checkDate() 已删除 - 运营报告相关功能，不属于此脚本


# insertDB() 已删除 - 运营报告相关功能，不属于此脚本


# upload_file() 已删除 - 旧版文件上传，只被已删除的 upload_dir() 使用


# crawl_pdf1() 已删除 - 运营报告相关功能，不属于此脚本

if __name__ == "__main__":

    # 从FTP读取上次更新时间
    timestamp = read_ftp_file(ftp, UPDATE_LOG_PATH)
    print("上次更新的日期为 " + timestamp)
    
    firstTimestamp, urls = get_html(timestamp)
    
    # save urls in abn_urls.txt line by line, overwriting the file
    with open('abn_urls.txt', 'w') as f:
        for url in urls:
            f.write(url + '\n')

    gmProds, smProds, qtProds = [], [], []
    gmEx = set()

    for idx, url in enumerate(urls, 1):
        print(f"\n{'='*80}")
        print(f"处理进度: {idx}/{len(urls)}")
        print(f"{'='*80}")
        
        if len(url.split(':')) < 3:
            print(f"⚠️ 跳过格式错误的URL: {url}")
            continue

        url_path = url.split(':')[1]
        print(f"处理URL: {url_path}")
        
        try:
            res = newProd(url_path)
            if res:
                if res[0] == '公开发行':
                    gmProds.append(res[1])
                    print(f"✓ 公开发行产品: {res[1]}")
                elif res[0] == '定向发行':
                    smProds.append(res[1])
                    print(f"✓ 定向发行产品: {res[1]}")
                else:
                    qtProds.append(res[1])
                    print(f"✓ 其他类型产品: {res[1]}")
            else:
                print(f"⚠️ newProd返回None，跳过此产品")
        except Exception as e:
            print(f"❌ 处理产品时出错: {e}")
            import traceback
            traceback.print_exc()
            print(f"继续处理下一个产品...")

    # 更新时间到FTP（只有在有新数据时才更新）
    if urls:
        print(f"\n{'='*80}")
        print(f"更新时间戳到FTP")
        print(f"{'='*80}")
        print(f"上次时间: {timestamp}")
        print(f"本次时间: {firstTimestamp}")
        print(f"Writing latest date time {firstTimestamp} to {UPDATE_LOG_PATH}")
        firstTimestamp_str = firstTimestamp.strftime("%Y-%m-%d %H:%M:%S")

        with io.BytesIO(firstTimestamp_str.encode("utf-8")) as bio:
            ftp.storbinary(f"STOR {UPDATE_LOG_PATH}", bio)
        print("✓ 时间戳更新成功！")
    else:
        print("\n⚠️ 没有新数据，不更新时间戳")
    
    print(f"\n{'='*80}")
    print("✅ 脚本执行完成！")
    print(f"{'='*80}")
    print(f"处理了 {len(urls)} 个URL")
    print(f"公募产品: {len(gmProds)}, 私募产品: {len(smProds)}, 其他: {len(qtProds)}")
    print(f"{'='*80}\n")
    

