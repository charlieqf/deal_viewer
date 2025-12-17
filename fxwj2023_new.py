import smtplib
import socket
from email.mime.text import MIMEText
from dateutil.parser import parse
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import requests
import os
import io
import sys
import random
from datetime import datetime
import logging
import pypinyin
import pymssql
import ftplib
import pyodbc
import chardet

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from urllib.parse import urljoin
from urllib.parse import quote
import threading

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("发行文件日志记录.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
now_time = str(datetime.now())


# FTP server details 10.0.0.114
FTP_HOST = "113.125.202.171"
FTP_PORT = 11421
FTP_USER = "dv"
FTP_PASS = "246qweASD@"
FTP_HOME_DIR = "."

# FTP2 server details 192.168.1.211
FTP2_HOST = "113.125.202.171"
FTP2_PORT = 21121
FTP2_USER = "gsuser"
FTP2_PASS = "Password01"
FTP2_HOME_DIR = "."

# 114
UPDATE_LOG_PATH = "/Products_log/更新时间TXT记录/发行文件更新时间.txt"
FTP_FOLDER_PATH = "/Products_log/银行间债券市场更新数据"
INCREMENT_FOLDER_PATH = "/增量文档"
# 211
DV_FOLDER_PATH = "/DealViewer/TrustAssociatedDoc"
DV_CREDIT_RATING_FOLDER = "ProductCreditRatingFiles"
DV_RELEASE_INSTRUCTION_FOLDER = "ProductReleaseInstructions"


def list_ftp_directory_with_retry(ftp, path, retries=5):
    """列出FTP目录内容，带有自动重试和重连功能"""
    for attempt in range(retries):
        try:
            return list_ftp_directory(ftp, path)
        except (ftplib.error_temp, ftplib.error_perm, ftplib.error_reply, BrokenPipeError, TimeoutError, ConnectionError, socket.timeout, OSError) as e:
            print(
                f"Error listing directory {path}: {e}. Retrying {retries - attempt - 1} more times."
            )
            
            # 对于连接相关错误，尝试重新连接FTP
            if isinstance(e, (BrokenPipeError, TimeoutError, ConnectionError, socket.timeout, OSError)):
                try:
                    print("Connection error detected, attempting to reconnect FTP...")
                    # 保存连接信息
                    host = ftp.host
                    port = ftp.port
                    user = ftp._user if hasattr(ftp, '_user') else FTP_USER  # 备用
                    passwd = ftp._passwd if hasattr(ftp, '_passwd') else FTP_PASS  # 备用
                    
                    # 尝试关闭旧连接
                    try:
                        ftp.close()
                    except:
                        pass  # 忽略关闭错误
                    
                    # 重新连接
                    print(f"Reconnecting to {host}:{port}...")
                    ftp.connect(host, port, timeout=60)
                    ftp.login(user, passwd)
                    print("FTP reconnection successful")
                except Exception as reconnect_error:
                    print(f"Failed to reconnect to FTP: {reconnect_error}")
            
            # 使用指数退避策略
            wait_time = 5 * (2 ** attempt)  # 5, 10, 20, 40...
            if wait_time > 60:
                wait_time = 60  # 最长等待60秒
            print(f"Waiting {wait_time} seconds before next attempt...")
            time.sleep(wait_time)

    # 所有重试失败
    raise Exception(f"Failed to list directory {path} after {retries} attempts")


def list_ftp_directory(ftp, path):
    """List files and directories in the given FTP path."""
    print("Listing directory:", path)
    
    try:
        # 切换到目标目录
        ftp.cwd(path)
    except ftplib.error_perm as e:
        # 处理目录不存在的情况
        if "550" in str(e):
            print(f"Directory {path} does not exist")
            return []
        else:
            raise
    
    try:
        # 尝试设置更长的超时时间
        if hasattr(ftp, 'sock') and ftp.sock:
            ftp.sock.settimeout(120)  # 2分钟超时
            
        # 先尝试使用NLST命令
        try:
            # 使用retrlines而非retrbinary，更安全
            items = []
            ftp.retrlines("NLST", items.append)
        except Exception as e:
            # 如果NLST失败，尝试LIST命令
            print(f"NLST failed: {e}, trying LIST command instead")
            items = []
            ftp.retrlines("LIST", items.append)
            # 从完整的LIST输出中提取文件名
            items = [item.split()[-1] for item in items if item.strip()]
    finally:
        # 恢复原始超时设置
        if hasattr(ftp, 'sock') and ftp.sock:
            ftp.sock.settimeout(None)
    
    # 清理列表，移除空项
    result = [item for item in items if item and item.strip()]
    print(f"Found {len(result)} items in {path}")
    return result


def read_ftp_file(ftp, file_path):
    with io.BytesIO() as bio:
        ftp.retrbinary(f"RETR {file_path}", bio.write)
        bio.seek(0)
        return bio.read().decode("utf-8")


def store_data_to_ftp_with_retry(ftp, data, file_path, retries=5):
    """将数据存储到FTP，带有自动重试和重连功能"""
    original_timeout = None
    for attempt in range(retries):
        try:
            # 设置更长的超时时间
            if hasattr(ftp, 'sock') and ftp.sock:
                original_timeout = ftp.sock.gettimeout()
                ftp.sock.settimeout(120)  # 2分钟超时
            
            # 创建字节流并上传
            print(f"正在保存数据到 {file_path} =====> (尝试 {attempt+1}/{retries})")
            with io.BytesIO(data.encode("utf-8")) as bio:
                ftp.storbinary(f"STOR {file_path}", bio)
            
            print(f"成功保存数据到 {file_path}")
            
            # 恢复原始超时时间
            if hasattr(ftp, 'sock') and ftp.sock and original_timeout:
                ftp.sock.settimeout(original_timeout)
                
            return True
                
        except (ftplib.error_temp, ftplib.error_perm, ftplib.error_reply, BrokenPipeError, TimeoutError, ConnectionError, socket.timeout, OSError) as e:
            # 恢复原始超时时间
            if hasattr(ftp, 'sock') and ftp.sock and original_timeout:
                ftp.sock.settimeout(original_timeout)
                
            # 忽略"200 OK"错误，这实际上意味着操作成功
            if '200 OK' in str(e):
                print(f"忽略带有'200 OK'的误导性错误: {e}")
                return True
                
            print(f"保存数据到 {file_path} 时出错: {e}。还将重试 {retries - attempt - 1} 次。")
            
            # 处理连接相关错误，尝试重新连接FTP
            if isinstance(e, (BrokenPipeError, TimeoutError, ConnectionError, socket.timeout, OSError)):
                try:
                    print("检测到连接错误，尝试重新连接FTP...")
                    # 保存连接信息
                    host = ftp.host
                    port = ftp.port
                    user = ftp._user if hasattr(ftp, '_user') else FTP_USER  # 备用
                    passwd = ftp._passwd if hasattr(ftp, '_passwd') else FTP_PASS  # 备用
                    
                    # 尝试关闭旧连接
                    try:
                        ftp.close()
                    except:
                        pass  # 忽略关闭错误
                    
                    # 重新连接
                    print(f"重新连接到 {host}:{port}...")
                    ftp.connect(host, port, timeout=120)  # 更长的超时时间
                    ftp.login(user, passwd)
                    print("FTP重连成功")
                except Exception as reconnect_error:
                    print(f"重连FTP失败: {reconnect_error}")
            
            # 使用指数退避策略
            wait_time = 5 * (2 ** attempt)  # 5, 10, 20, 40...
            if wait_time > 60:
                wait_time = 60  # 最长等待时间60秒
            print(f"等待 {wait_time} 秒后重试...")
            time.sleep(wait_time)
    
    # 所有重试都失败
    raise Exception(f"在 {retries} 次尝试后仍然无法保存数据到 {file_path}")


def store_data_to_ftp(ftp, data, file_path):
    # 使用带有重试功能的版本
    return store_data_to_ftp_with_retry(ftp, data, file_path)


print("Connect to FTP server ", FTP_HOST, FTP_PORT)
ftp = ftplib.FTP()
ftp.connect(FTP_HOST, FTP_PORT, timeout=600)
ftp.login(FTP_USER, FTP_PASS)
# ftp.cwd(FTP_HOME_DIR)


def enable_utf8(ftp):
    response = ftp.sendcmd("OPTS UTF8 ON")
    if "200" in response:
        print("UTF-8 encoding enabled on the FTP server.")
    else:
        print("Failed to enable UTF-8 encoding on the FTP server.")


print("Connect to FTP server ", FTP2_HOST, FTP2_PORT)
ftp2 = ftplib.FTP()
ftp2.connect(FTP2_HOST, FTP2_PORT, timeout=600)
ftp2.login(FTP2_USER, FTP2_PASS)
enable_utf8(ftp2)
ftp2.encoding = "utf-8"


def keep_alive(ftp, interval):
    while True:
        try:
            # 尝试发送NOOP命令保持连接活跃
            ftp.voidcmd("NOOP")
        except Exception as e:
            # 捕获所有异常，防止线程崩溃
            print(f"FTP keep-alive error: {e}")
            # 如果是严重错误，可能需要重新连接FTP
            try:
                # 检查FTP连接是否仍然活跃
                ftp.voidcmd("PWD")
            except:
                print("FTP connection appears to be broken, attempting reconnect...")
                try:
                    # 尝试重新连接FTP
                    if isinstance(ftp, ftplib.FTP):
                        host = ftp.host
                        port = ftp.port
                        user = ftp._user
                        passwd = ftp._passwd
                        ftp.connect(host, port, timeout=600)
                        ftp.login(user, passwd)
                        print("FTP reconnection successful")
                except Exception as reconnect_error:
                    print(f"Failed to reconnect to FTP: {reconnect_error}")
        # 无论是否发生异常，都等待指定的时间
        time.sleep(interval)


# Start a thread to keep the connection alive
keep_alive_thread = threading.Thread(
    target=keep_alive, args=(ftp, 60)
)  # NOOP every 5 minutes
keep_alive_thread.daemon = True
keep_alive_thread.start()

keep_alive_thread2 = threading.Thread(
    target=keep_alive, args=(ftp2, 60)
)  # NOOP every 5 minutes
keep_alive_thread2.daemon = True
keep_alive_thread2.start()


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

# Smartproxy credentials and proxy URL
# username = "splci64blr"
# password = "6j0z1hrwFM4LbdheZ_"
# proxy_url = f"http://{username}:{password}@gate.smartproxy.com:10001"

# 解析 ProxyJet 字符串
proxy_string = "proxy-jet.io:1010:2506034iYZQ-resi_region-AU_Newsouthwales_Parramatta:rUGciFpmX7CwT12"
parts = proxy_string.split(":")
hostname = parts[0]
port = parts[1]
username = parts[2]
password = parts[3]

# 创建代理 URL (格式: http://username:password@hostname:port)
proxy_url = f"http://{username}:{password}@{hostname}:{port}"

proxies = {
    "http": proxy_url,
    "https": proxy_url,
}

def use_selenium(proxies):
    # proxy test
    print("testing proxies")
    test_url = "https://ip.smartproxy.com/json"
    response = requests.get(test_url, proxies=proxies)
    print(response.status_code, response.json())

    print("connecting to the website...")
    driver.get(
        "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/"
    )
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    time.sleep(3)  # Wait for the page to load

    print(driver.title)

    last_date = read_ftp_file(ftp, UPDATE_LOG_PATH)
    print("上次更新的日期为 " + last_date)

    url = "https://www.chinabond.com.cn/cbiw/trs/getDocsByConditions"
    data = {
        "childChnlName": "发行文件",
        "keywords": "",
        "pageNum": 1,
        "isHasAppendix": 1,
        "pageSize": 50,
        "parentChnlId": 948,
        "noticeYear": "",
        "fxrId": "",
        "zcxsId": "",
    }

    # Define headers for the request
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    }

    # Configure the proxy
    proxies = {
        "http": proxy_url,
        "https": proxy_url,
    }

    # Send the POST request
    response = requests.post(url, data=data, headers=headers, proxies=proxies)
    # print(response.status_code, response.reason, response.text, response.headers)
    response_data = response.json()

    products = []
    # 初始化latest_date_time变量，避免UnboundLocalError
    latest_date_time = last_date
    if response_data["success"]:
        list_data = response_data["data"]["data"]["list"]

        # get the latest ShengXiaoShiJian from all items in list_data (parse the date string to datetime object)
        latest_date_time = max(
            [parse(item["ShengXiaoShiJian"]) for item in list_data]
        ).strftime("%Y-%m-%d %H:%M:%S")

        # iterate through the list_data where DocTitle contains '发行文件'
        for item in [item for item in list_data if "发行文件" in item["DocTitle"]]:
            """
            {
                "DOCCONTENT": "",
                "ShengXiaoShiJian": "2024-06-05 11:20:59",
                "DocTitle": "兴瑞2024年第三期不良资产支持证券发行文件",
                "docid": 853811574,
                "FaXingQiShu": "null",
                "DOCPUBURL": "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/zjzczq_ABS/ABS_fxwj_ath/202406/t20240605_853811574.html",
                "MetaDataId": 853811574,
                "OriginDocId": 853811574,
                "recid": 1047962,
                "appendixIds": "1416267=P020240605408595602651.pdf=兴瑞2024年第三期不良资产支持证券承销团成员名单.pdf,1416268=P020240605408595801526.pdf=兴瑞2024年第三期不良资产支持证券发行办法.pdf,1416269=P020240605408595913336.pdf=兴瑞2024年第三期不良资产支持证券发行公告.pdf,1416270=P020240605408596015054.pdf=兴瑞2024年第三期不良资产支持证券发行说明书.pdf,1416271=P020240605408596262846.pdf=兴瑞2024年第三期不良资产支持证券信托公告.pdf,1416272=P020240605408596445818.pdf=兴瑞2024年第三期不良资产支持证券《中债资信信用评级报告》.pdf,1416273=P020240605408596747644.pdf=兴瑞2024年第三期不良资产支持证券 《惠誉博华信用评级报告》.pdf",
                "FaXingNianFen": "2023"
            },
            there are multiple pdf files in the appendixIds, we need to extract the pdf path from DOCPUBURL and appendixIds
            each pdf path = https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/zjzczq_ABS/ABS_fxjg_ath/202406/P020240604363380159323.pdf
            pdf_path = DOCPUBURL remove the bits after the last /, then add part of appendixIds
            """
            issue_time = item.get("ShengXiaoShiJian", "")
            doc_title = item.get("DocTitle", "")
            doc_url = item.get("DOCPUBURL", "")
            appendix_ids = item.get("appendixIds", "")

            pdf_path_home = doc_url.rsplit("/", 1)[0]
            print("pdf_path_home =", pdf_path_home)
            # get multiple pdf paths
            appendix_ids = appendix_ids.split(",")
            pdf_paths = []

            for appendix_id in appendix_ids:
                # 1416267=P020240605408595602651.pdf=兴瑞2024年第三期不良资产支持证券承销团成员名单.pdf
                pdf_name = appendix_id.split("=")[1]
                target_pdf_name = appendix_id.split("=")[2]
                pdf_path = f"{pdf_path_home}/{pdf_name}"
                pdf_paths.append((pdf_path, target_pdf_name))

            # save the data to products

            if parse(issue_time) > parse(last_date):
                products.append(
                    {
                        "issue_time": issue_time,
                        "doc_title": doc_title,
                        "doc_url": doc_url,
                        "pdf_paths": pdf_paths,
                    }
                )

    # Process the products
    print("Processing products... (total: %d)" % len(products))
    # print(products)
    update_pdf_new(products)

    # write latest_date_time to the update log file on the FTP server
    print("Writing latest date time {} to".format(latest_date_time), UPDATE_LOG_PATH)
    with io.BytesIO(latest_date_time.encode("utf-8")) as bio:
        ftp.storbinary(f"STOR {UPDATE_LOG_PATH}", bio)


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
        if response.status_code == 200 and response.headers.get("Content-Type") == "application/pdf":
            print("Download successful without proxy")
            return True, response.content
        
        # Check if this is an IP-related failure
        ip_related_failure = False
        if response.status_code in [403, 429, 451]:
            ip_related_failure = True
        elif response.status_code != 200:
            # Check response text for IP blocking messages
            ip_block_indicators = ["blocked", "forbidden", "access denied", "IP", "地址被禁止", "访问受限", "访问被拒绝"]
            response_text = response.text.lower()
            for indicator in ip_block_indicators:
                if indicator.lower() in response_text:
                    ip_related_failure = True
                    break
        
        if not ip_related_failure:
            # Failed but not due to IP issues
            print(f"Failed to download without proxy. Status: {response.status_code}. Not IP related.")
            return False, response.text
    except Exception as e:
        print(f"Error during non-proxy download attempt: {e}")
        # For connection errors, assume it could be IP related
        if isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
            print("Connection error - might be IP related")
        else:
            # For other types of errors, return the error
            return False, str(e)
    
    # If we got here, either there was an IP-related failure or an exception occurred
    # Try again with SmartProxy
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


def create_dir_on_ftp(ftp, dir, folder):
    # check if the folder exists on the ftp server, if not, create the folder
    folder_path = os.path.join(dir, folder)
    if folder not in list_ftp_directory_with_retry(ftp, dir):
        print("create new folder: ", folder_path)
        ftp.mkd(folder_path)
    else:
        print(folder, "already exists in", dir, "on FTP")

    return folder_path

def update_pdf_new(products):
    month = str(datetime.now().month)
    month_folder_path = create_dir_on_ftp(ftp, INCREMENT_FOLDER_PATH, month)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    cache_folder = os.path.join(current_dir, "file_cache")
    if not os.path.exists(cache_folder):
        os.makedirs(cache_folder)

    for product in products:
        title = product["doc_title"]
        web_date = product["issue_time"]
        product_name = title.split("证券")[0] + "证券"
        pdf_paths = product["pdf_paths"]
        success_file = os.path.join(cache_folder, f"{product_name}.success")

        if os.path.exists(success_file):
            #print("Product already processed:", product_name)
            continue

        # if parse(web_date) < datetime.strptime('2024-06-12', "%Y-%m-%d") and parse(web_date) >= datetime.strptime('2024-06-14', "%Y-%m-%d"):
        #     continue
        
        # if '兴瑞2024年' not in product_name and '招元和萃2024年' not in product_name and '臻粹2024年' not in product_name:
        #     continue

        print("处理产品:", product_name)

        # 在数据库中新建产品（如尚不存在）
        trust_code = create_new_product(product_name)

        print("TrustCode:", trust_code)

        # create necessary folders on FTP server
        product_folder = product_name
        product_folder_path = create_dir_on_ftp(ftp, FTP_FOLDER_PATH, product_folder)
        increment_product_folder_path = create_dir_on_ftp(
            ftp, month_folder_path, product_folder
        )
        dv_product_folder_path = create_dir_on_ftp(ftp2, DV_FOLDER_PATH, trust_code)
        ftp_dv_credit_rating_folder_path = create_dir_on_ftp(
            ftp2, dv_product_folder_path, DV_CREDIT_RATING_FOLDER
        )
        ftp_dv_release_instruction_folder_path = create_dir_on_ftp(
            ftp2, dv_product_folder_path, DV_RELEASE_INSTRUCTION_FOLDER
        )

        for path in pdf_paths:
            web_pdf_path = path[0]
            target_pdf_name = path[1]

            # 如果target_pdf_name含有"|"或"||",则删除
            if "|" in target_pdf_name or "||" in target_pdf_name:
                target_pdf_name = target_pdf_name.replace("|", "")
                print("target_pdf_name after replace:", target_pdf_name)
            # 如果target_pdf_name不包含"."，则添加".pdf"
            if "." not in target_pdf_name:
                target_pdf_name = target_pdf_name + ".pdf"
                print("target_pdf_name after add .pdf:", target_pdf_name)

            # check if file exists in the cache folder; download if not
            cache_pdf_path = os.path.join(cache_folder, target_pdf_name)
            file_exists = os.path.exists(cache_pdf_path)

            if not file_exists:
                print("Downloading PDF from", web_pdf_path, "to cache folder ====>")
                try:
                    result, conctent = get_web_pdf_content(web_pdf_path)
                    if result:
                        with open(cache_pdf_path, "wb") as f:
                            f.write(conctent)
                        file_exists = True

                except Exception as e:
                    print("Error downloading PDF file:", e)
            else:
                print("PDF file already exists in cache folder")

            if file_exists:
                # 上传到product_folder
                ftp_pdf_path = os.path.join(product_folder_path, target_pdf_name)
                upload_file_to_ftp(
                    ftp,
                    cache_pdf_path,
                    product_folder_path,
                    ftp_pdf_path,
                    target_pdf_name,
                )

                # 上传到month_file_folder
                ftp_pdf_path = os.path.join(
                    increment_product_folder_path, target_pdf_name
                )
                upload_file_to_ftp(
                    ftp,
                    cache_pdf_path,
                    increment_product_folder_path,
                    ftp_pdf_path,
                    target_pdf_name,
                )

                # 写入192.168.1.211 FTP
                if ".pdf" in target_pdf_name:
                    if "评级报告" in target_pdf_name:
                        ftp_dir = ftp_dv_credit_rating_folder_path
                        sql_file_path = ftp_dv_credit_rating_folder_path + '/'

                        # 文件上传
                        file_type = "ProductCreditRatingFiles"
                        file_name = target_pdf_name
                        ftp_pdf_path = os.path.join(ftp_dir, file_name)

                        print("上传评级报告到211FTP:", ftp_pdf_path)
                        upload_file_to_ftp(
                            ftp2, cache_pdf_path, ftp_dir, ftp_pdf_path, file_name
                        )

                        print("写入数据库记录")
                        insert_to_db(
                            conn, trust_code, sql_file_path, file_name, file_type
                        )
                    else:
                        ftp_dir = ftp_dv_release_instruction_folder_path
                        sql_file_path = ftp_dv_release_instruction_folder_path + '/'

                        # 文件上传
                        file_type = "ProductReleaseInstructions"
                        file_name = target_pdf_name
                        ftp_pdf_path = os.path.join(ftp_dir, file_name)

                        print("上传发行文件到211FTP:", ftp_pdf_path)
                        upload_file_to_ftp(
                            ftp2, cache_pdf_path, ftp_dir, ftp_pdf_path, file_name
                        )

                        print("写入数据库记录")
                        insert_to_db(
                            conn, trust_code, sql_file_path, file_name, file_type
                        )

            time.sleep(3)

        try:
            if "更正" in product_name or "更新" in product_name:
                print(product_name, "更正更新的产品不插入数据库的披露表跟状态表")
            else:
                insert_task_info(trust_code, web_date)

                # create an empty trust_code.txt file in the product_folder_path on the FTP server
                txt = trust_code + ".txt"
                ftp_trust_code_txt_path = os.path.join(product_folder_path, txt)
                # create this empty txt file on FTP server at ftp_trust_code_txt_path
                with io.BytesIO(b"") as bio:
                    ftp.storbinary(f"STOR {ftp_trust_code_txt_path}", bio)

                # create a 'title.success' file in the cache folder, if not exists
                if not os.path.exists(success_file):
                    with open(success_file, "w") as f:
                        f.write("success")


        except:
            print(trust_code, "披露信息插入失败!")

        print('产品新建完成')

def upload_file_to_ftp_with_retry(ftp, local_file_path, ftp_folder, ftp_file_path, file_name, retries=5):
    """上传文件到FTP，带有重试和自动重连功能"""
    original_timeout = None
    for attempt in range(retries):
        try:
            # 设置更长的超时时间
            if hasattr(ftp, 'sock') and ftp.sock:
                original_timeout = ftp.sock.gettimeout()
                ftp.sock.settimeout(120)  # 2分钟超时
            
            # 检查文件在FTP上是否存在
            file_exists = False
            try:
                dir_contents = list_ftp_directory_with_retry(ftp, ftp_folder)
                file_exists = file_name in dir_contents
            except Exception as e:
                # 如果遇到"200 OK"错误，忽略它并继续
                if '200 OK' in str(e):
                    print(f"忽略误导性的目录列表错误: {e}")
                else:
                    raise
                    
            if not file_exists:
                print(f"Writing {local_file_path} to {ftp_file_path} =====> (Attempt {attempt+1}/{retries})")
                try:
                    with open(local_file_path, "rb") as f:
                        ftp.storbinary(f"STOR {ftp_file_path}", f)
                    print(f"Successfully uploaded {ftp_file_path}")
                    
                    # 恢复原始超时时间
                    if hasattr(ftp, 'sock') and ftp.sock and original_timeout:
                        ftp.sock.settimeout(original_timeout)
                        
                    return True
                except Exception as upload_error:
                    # 如果错误消息包含"200 OK"，实际上是成功的
                    if '200 OK' in str(upload_error):
                        print(f"Upload successful despite error message: {upload_error}")
                        
                        # 恢复原始超时时间
                        if hasattr(ftp, 'sock') and ftp.sock and original_timeout:
                            ftp.sock.settimeout(original_timeout)
                            
                        return True
                    # 否则重新抛出异常
                    raise
            else:
                print(f"File already exists on FTP in folder: {ftp_folder}")
                
                # 恢复原始超时时间
                if hasattr(ftp, 'sock') and ftp.sock and original_timeout:
                    ftp.sock.settimeout(original_timeout)
                    
                return True
                
        except (ftplib.error_temp, ftplib.error_perm, ftplib.error_reply, BrokenPipeError, TimeoutError, ConnectionError, socket.timeout, OSError) as e:
            # 恢复原始超时时间
            if hasattr(ftp, 'sock') and ftp.sock and original_timeout:
                ftp.sock.settimeout(original_timeout)
                
            # 忽略"200 OK"错误，这实际上意味着操作成功
            if '200 OK' in str(e):
                print(f"Ignoring misleading error with '200 OK': {e}")
                return True
                
            print(f"Error uploading file {ftp_file_path}: {e}. Retrying {retries - attempt - 1} more times.")
            
            # 处理连接相关错误，尝试重新连接FTP
            if isinstance(e, (BrokenPipeError, TimeoutError, ConnectionError, socket.timeout, OSError)):
                try:
                    print("Connection error detected, attempting to reconnect FTP...")
                    # Save connection info
                    host = ftp.host
                    port = ftp.port
                    user = ftp._user if hasattr(ftp, '_user') else FTP_USER  # Fallback
                    passwd = ftp._passwd if hasattr(ftp, '_passwd') else FTP_PASS  # Fallback
                    
                    # Try to close old connection
                    try:
                        ftp.close()
                    except:
                        pass  # Ignore close errors
                    
                    # Reconnect
                    print(f"Reconnecting to {host}:{port}...")
                    ftp.connect(host, port, timeout=120)  # Longer timeout for upload
                    ftp.login(user, passwd)
                    print("FTP reconnection successful")
                except Exception as reconnect_error:
                    print(f"Failed to reconnect to FTP: {reconnect_error}")
            
            # Use exponential backoff strategy
            wait_time = 5 * (2 ** attempt)  # 5, 10, 20, 40...
            if wait_time > 60:
                wait_time = 60  # Maximum wait of 60 seconds
            print(f"Waiting {wait_time} seconds before next attempt...")
            time.sleep(wait_time)
    
    # All retries failed
    raise Exception(f"Failed to upload file {ftp_file_path} after {retries} attempts")

def upload_file_to_ftp(ftp, local_file_path, ftp_folder, ftp_file_path, file_name):
    # Use the retry version
    return upload_file_to_ftp_with_retry(ftp, local_file_path, ftp_folder, ftp_file_path, file_name)

# 转换中文数值为阿拉伯数值
def conversion(str):
    if type(str) == int:
        return str
    else:

        zhong = {
            "零": 0,
            "一": 1,
            "二": 2,
            "三": 3,
            "四": 4,
            "五": 5,
            "六": 6,
            "七": 7,
            "八": 8,
            "九": 9,
        }
        danwei = {"十": 10, "百": 100, "千": 1000, "万": 10000}
        num = 0
        if len(str) == 0:
            return 0
        if len(str) == 1:
            if str == "十":
                return 10
            num = zhong[str]
            return num
        temp = 0
        if str[0] == "十":
            num = 10
        for i in str:
            if i == "零":
                temp = zhong[i]
            elif i == "一":
                temp = zhong[i]
            elif i == "二":
                temp = zhong[i]
            elif i == "三":
                temp = zhong[i]
            elif i == "四":
                temp = zhong[i]
            elif i == "五":
                temp = zhong[i]
            elif i == "六":
                temp = zhong[i]
            elif i == "七":
                temp = zhong[i]
            elif i == "八":
                temp = zhong[i]
            elif i == "九":
                temp = zhong[i]
            if i == "十":
                temp = temp * danwei[i]
                num += temp
            elif i == "百":
                temp = temp * danwei[i]
                num += temp
            elif i == "千":
                temp = temp * danwei[i]
                num += temp
            elif i == "万":
                temp = temp * danwei[i]
                num += temp
        if (
            str[len(str) - 1] != "十"
            and str[len(str) - 1] != "百"
            and str[len(str) - 1] != "千"
            and str[len(str) - 1] != "万"
        ):
            num += temp
        return num


# Trust表插入新产品数据
def create_new_product(product_name):
    global trust_code
    if "消费贷款" in product_name:
        FCode = "ConsumerLoan"
    elif "信用卡" in product_name:
        FCode = "ConsumerLoan"
    elif "信贷资产" in product_name:
        FCode = "CreditLoan"
    elif "住房抵押贷款" in product_name:
        FCode = "RMBS"
    elif "汽车抵押贷款" in product_name:
        FCode = "AUTO"
    elif "汽车贷款" in product_name:
        FCode = "AUTO"
    elif "汽车分期贷款" in product_name:
        FCode = "AUTO"
    elif "微小企业贷款" in product_name:
        FCode = "SmallLoan"
    elif "租赁资产" in product_name:
        FCode = "Lease"
    elif "不良资产" in product_name:
        FCode = "NPL"
    else:
        FCode = ""

    if product_name == "华驭第十六期汽车抵押贷款支持证券":
        trust_code = "HuaYu2025-16"
        trust_name_short = "华驭2025-16"
        trust_name = "华驭第十六期汽车抵押贷款支持证券"
    else:
        try:
            s = ""
            for i in pypinyin.pinyin(product_name, style=pypinyin.NORMAL):
                i = i[0].title()
                s += "".join(i)

            s = s.split("Nian")[0]

            sp_filename = product_name.split("第")[1]
            nper = sp_filename.split("期")[0]
            conversion_nper = conversion(nper)
            s_trust_code = s + "-" + str(conversion_nper)

            trust_code_1 = s_trust_code.split("2025")[0]
            trust_code_2 = s_trust_code.split("2025")[-1]

            trust_code = trust_code_1 + "_" + FCode + "2025" + trust_code_2
            print(trust_code)

            if "更正" in product_name or "更新" in product_name:
                return
            splitname = product_name.split("年")[0]
            trust_name_short = splitname + "-" + str(conversion_nper)
            trust_name = product_name.split("发行文件")[0]
        except:
            print("TrustCode\TrustName获取失败")
            return

    # if Product_name=='华驭第十四期汽车抵押贷款支持证券发行文件':
    #     TrustName='华驭第十四期汽车抵押贷款支持证券'
    #     TrustNameShort='华驭-14'
    #     TrustCode='HuaYu_AUTO-14'

    def AType():
        if "住房" in trust_name:
            AssetType = "HouseLoan"
        elif ("个人消费" or "信用卡分期") in trust_name:
            AssetType = "ConsumerLoan"
        elif "汽车" in trust_name:
            AssetType = "CarLoan"
        elif "信贷" in trust_name:
            AssetType = "CreditLoan"
        elif "微小企业" in trust_name:
            AssetType = "SmallLoan"
        elif "租赁资产" in trust_name:
            AssetType = "Lease"
        elif "不良资产" in trust_name:
            AssetType = "NPL"
        else:
            AssetType = "null"
        return AssetType

    # conn = pymssql.connect(host='113.125.202.171,52482', user='sa', password='PasswordGS2017',
    #                        database='PortfolioManagement', charset='utf8')
    b2 = conn.cursor()

    # Enhanced check: Check Name, Code, and ShortName to prevent UQ_TrustName violation
    sql_check = "select TrustCode from TrustManagement.Trust where TrustCode='{}' or TrustName=N'{}' or TrustNameShort=N'{}'".format(
        trust_code, trust_name, trust_name_short
    )
    print(sql_check)
    b2.execute(sql_check)
    existing_product = b2.fetchone()
    if existing_product:
        print("产品已存在(Code/Name/ShortName重复):", existing_product[0])
        return existing_product[0]

    sql1 = " select max(TrustId)+1 from TrustManagement.Trust where TrustId<50000"
    b2.execute(sql1)
    TrustId = b2.fetchone()[0]

    print(
        "assigned new TrustID: ",
        TrustId,
        "for TrustCode: ",
        trust_code,
        "TrustName: ",
        product_name,
    )

    sql = """
        SET IDENTITY_INSERT TrustManagement.Trust ON ;
        insert into TrustManagement.Trust(TrustId,TrustCode,TrustName,TrustNameShort,IsMarketProduct,TrustStatus) values({},'{}',N'{}',N'{}',1,'Duration');
        SET IDENTITY_INSERT TrustManagement.Trust OFF ;
    """.format(
        TrustId, trust_code, trust_name, trust_name_short
    )

    b2.execute(sql)

    conn.commit()

    sql2 = """
        SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust ON ;
        insert into FixedIncomeSuite.Analysis.Trust(TrustId,TrustCode,TrustName)
        select TrustId,TrustCode,TrustName from [DV].[view_Products] where TrustId={};
        SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust OFF ;
    """.format(
        TrustId
    )
    b2.execute(sql2)
    conn.commit()
    sql2 = "insert into TrustManagement.TrustInfoExtension(TrustId, StartDate, EndDate, ItemId, ItemCode, ItemValue) values ({}, GETDATE(), null, null, 'MarketCategory','CAS'),({}, GETDATE(), null, null, 'RegulatoryOrg','CBIRC'),({}, GETDATE(), null, null, 'MarketPlace', 'InterBank'),({}, GETDATE(), null, null, 'AssetType','{}'),({}, GETDATE(), null, null, 'BasicAssetType','Others'),({}, GETDATE(), null, null, 'CollectionMethod', 'PublicOffering')".format(
        TrustId, TrustId, TrustId, TrustId, AType(), TrustId, TrustId
    )
    b2.execute(sql2)
    conn.commit()

    # 调用函数插入最大期数表
    instrtMaxNper(TrustId)

    return trust_code


def upload_to_ftp_with_retry(ftp, local_file_path, remote_dir, remote_file_path, retries=5):
    """Upload file to FTP with automatic retry and reconnection capability"""
    bufsize = 1024
    
    for attempt in range(retries):
        try:
            # Try to create directory if it doesn't exist
            try:
                ftp.mkd(remote_dir)
            except ftplib.error_perm:
                # Directory probably already exists
                print("FTP文件夹已存在")
            
            # Change to the target directory
            ftp.cwd(remote_dir)
            
            # Upload the file
            print(f"Uploading to FTP: {remote_file_path} (Attempt {attempt+1}/{retries})")
            with open(local_file_path, "rb") as fp:
                ftp.storbinary("STOR " + remote_file_path, fp, bufsize)
            
            print(f"Successfully uploaded to FTP: {remote_file_path}")
            return True
            
        except (ftplib.error_temp, ftplib.error_perm, ftplib.error_reply, BrokenPipeError, TimeoutError, ConnectionError, socket.timeout, OSError) as e:
            print(f"Error uploading file {remote_file_path}: {e}. Retrying {retries - attempt - 1} more times.")
            
            # For connection-related errors, try to reconnect to FTP
            if isinstance(e, (BrokenPipeError, TimeoutError, ConnectionError, socket.timeout, OSError)):
                try:
                    print("Connection error detected, attempting to reconnect FTP...")
                    # Save connection info
                    host = ftp.host
                    port = ftp.port
                    user = ftp._user if hasattr(ftp, '_user') else FTP_USER  # Fallback
                    passwd = ftp._passwd if hasattr(ftp, '_passwd') else FTP_PASS  # Fallback
                    
                    # Try to close old connection
                    try:
                        ftp.close()
                    except:
                        pass  # Ignore close errors
                    
                    # Reconnect
                    print(f"Reconnecting to {host}:{port}...")
                    ftp.connect(host, port, timeout=120)  # Longer timeout for upload
                    ftp.login(user, passwd)
                    print("FTP reconnection successful")
                except Exception as reconnect_error:
                    print(f"Failed to reconnect to FTP: {reconnect_error}")
            
            # Use exponential backoff strategy
            wait_time = 5 * (2 ** attempt)  # 5, 10, 20, 40...
            if wait_time > 60:
                wait_time = 60  # Maximum wait of 60 seconds
            print(f"Waiting {wait_time} seconds before next attempt...")
            time.sleep(wait_time)
    
    # All retries failed
    raise Exception(f"Failed to upload file {remote_file_path} after {retries} attempts")

def upload_to_ftp(ftp, local_file_path, remote_dir, remote_file_path):
    """Wrapper for the retry version of upload_to_ftp"""
    return upload_to_ftp_with_retry(ftp, local_file_path, remote_dir, remote_file_path)


def insert_to_db(conn, trust_code, sqlfilepath, filename, file_type):
    b1 = conn.cursor()
    sql = "select Trustid from DV.view_Products where TrustCode='{}'".format(trust_code)
    print(sql)
    b1.execute(sql)
    trust_id = b1.fetchone()[0]

    print(trust_id, file_type, sqlfilepath, filename)

    # check if the record already exists
    sql_check = "select 1 from DV.TrustAssociatedDocument where TrustId={} and FileName=N'{}'".format(
        trust_id, filename
    )
    print(sql_check)
    b1.execute(sql_check)
    if b1.fetchone():
        print("Record already exists in TrustAssociatedDocument: ", trust_id, filename)
        return

    sql = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values({},'{}','NULL','{}',N'{}','pdf',GETDATE(),'py')".format(
        trust_id, file_type, sqlfilepath, filename
    )

    print(sql)
    b1.execute(sql)

    print("记录插入成功")
    conn.commit()


# TrustAssociatedDocument表插入记录（DV页面显示）
def insert(TrustCode, sqlfilepath, filename, file_type, web_date):
    conn = pymssql.connect(
        host="172.16.6.143\mssql",
        user="sa",
        password="PasswordGS2017",
        database="PortfolioManagement",
        charset="utf8",
    )
    b1 = conn.cursor()

    select_Trustid = "select Trustid from DV.view_Products where TrustCode='{}'".format(
        TrustCode
    )
    b1.execute(select_Trustid)
    Trust_id = b1.fetchone()[0]
    print(Trust_id, file_type, sqlfilepath, filename)
    b2 = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values({},'{}','NULL','{}',N'{}','pdf',GETDATE(),'py')".format(
        Trust_id, file_type, sqlfilepath, filename
    )
    # print(sqlfilepath)
    # print(filename)
    print(b2)
    b1.execute(b2)
    print("记录插入成功")
    print("\n")
    conn.commit()

    # sql_Did = "select TrustDocumentID from DV.TrustAssociatedDocument where TrustId='{}'".format(Trust_id)
    # b1.execute(sql_Did)
    # TrustDocumentID = b1.fetchone()[0]

    # insert_Interaction="insert into TaskCollection.dbo.ProductStructureTask values({},{},'ProductReleaseInstructions',getdate(),getdate(),'yixianfeng',1)".format(TrustDocumentID,Trust_id)
    # b1.execute(insert_Interaction)
    # conn.commit()
    # print('中间表记录插入完成!')

    b1.close()
    conn.close()


# 更新最大期数
def instrtMaxNper(TrustId):
    b2 = conn.cursor()

    sql1 = "select TrustNameShort from TrustManagement.Trust where TrustId={}".format(
        TrustId
    )
    b2.execute(sql1)
    TrustNameShort = b2.fetchone()[0]

    sql = "insert into dbo.ReportMaxNper values({},N'{}',{},N'存续期')".format(
        TrustId, TrustNameShort, 0
    )
    b2.execute(sql)
    conn.commit()


# 披露信息插入(任务分配系统显示)
def insert_task_info(trust_code, web_date):
    # conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
    #                        database='PortfolioManagement', charset='utf8')
    b1 = conn.cursor()

    sql = f"select TrustId from TrustManagement.Trust where TrustCode='{trust_code}'"
    print(sql)
    b1.execute(sql)
    trust_id = b1.fetchone()[0]

    sql = f"select TrustDocumentID,FileName from PortfolioManagement.DV.TrustAssociatedDocument where TrustId={trust_id} and FileName like N'%说明书%'"
    print(sql)
    b1.execute(sql)
    TrustDocumentID, FileName = b1.fetchone()
    print("TrustDocumentID:", TrustDocumentID, ", FileName:", FileName)
    FileName = FileName[:-4]

    sql = f"select count(1) from TaskCollection.dbo.ProductsStateInformation where TrustId={trust_id} and TrustDocumentID={TrustDocumentID}"
    print(sql)
    b1.execute(sql)
    if b1.fetchone()[0] == 0:
        sql = f"insert into TaskCollection.dbo.ProductsStateInformation values({trust_id},{TrustDocumentID},1,9)"
        print(sql)
        b1.execute(sql)
        conn.commit()

    sql = f"select count(1) from dbo.DisclosureOfInformation where TrustId={trust_id} and FileName=N'{FileName}'"
    print(sql)
    b1.execute(sql)
    if b1.fetchone()[0] == 0:
        web_date = parse(str(web_date))
        sql = f"insert into dbo.DisclosureOfInformation(TrustId,TrustCode,FileName,DisclosureTime,FileType) values({trust_id},'{trust_code}',N'{FileName}','{web_date}',1)"
        print(sql)
        b1.execute(sql)
        conn.commit()

    print(trust_id, "任务信息插入完成")


def mail(gzProd, cmProd):
    msg_from = "fengyanyan@goldenstand.cn"  # 发送方邮箱
    passwd = "Fyy2516302813"  # 填入发送方邮箱的授权码
    msg_to1 = ["fengyanyan@goldenstand.cn"]

    content = (
        "{}\n".format(time.strftime("%Y-%m-%d"))
        + "债券网更新更正以及code重名产品情况:"
        + "\n"
    )

    content += "更新更正产品:" + str(len(gzProd)) + "个" + "\n"
    for i in gzProd:
        content += i
        content += "\n"

    content += "code重名产品:" + str(len(cmProd)) + "个" + "\n"
    for i in cmProd:
        content += i
        content += "\n"

    for msg_to in msg_to1:
        subject = "债券网更新更正以及code重名产品情况"  # 主题
        msg = MIMEText(content)
        msg["Subject"] = subject
        msg["From"] = msg_from
        msg["To"] = msg_to
        try:
            s = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
            s.login(msg_from, passwd)
            s.sendmail(msg_from, msg_to, msg.as_string())
            print("发送成功")
        except:
            print("发送失败")
        finally:
            s.quit()


if __name__ == "__main__":
    # execute cmd 'pkill -f chrome' to kill all chrome processes
    os.system("pkill -f chrome")

    options = webdriver.ChromeOptions()
    options.add_argument(
        'user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"'
    )
    options.add_argument(
        "--no-sandbox"
    )  # Bypass OS security model, needed for Chrome to run as root
    options.add_argument("--headless")  # Run headless Chrome to avoid GUI issues
    options.add_argument(
        "--disable-dev-shm-usage"
    )  # Overcome limited resource problems
    options.add_argument("--disable-gpu")  # Disable GPU acceleration
    options.add_argument(
        "--remote-debugging-port=9222"
    )  # To prevent DevToolsActivePort file doesn't exist error
    options.add_argument(f"--proxy-server={proxy_url}")
    options.add_argument("--log-level=3")  # Set log level to capture errors

    service = Service("/usr/bin/chromedriver")
    print("creating Chrome driver")
    driver = webdriver.Chrome(service=service, options=options)

    # chrome_options = Options()
    # chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36')
    # chrome_options.add_argument('--no-sandbox')  # Bypass OS security model, needed for Chrome to run as root
    # chrome_options.add_argument('--headless')  # Run headless Chrome to avoid GUI issues
    # chrome_options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems
    # chrome_options.add_argument('--disable-gpu')  # Disable GPU acceleration
    # chrome_options.add_argument('--remote-debugging-port=9222')  # To prevent DevToolsActivePort file doesn't exist error

    # driver = webdriver.Chrome(service=service, options=chrome_options)

    # test()
    use_selenium(proxies)

    ftp.close()
    ftp2.close()

    # options = webdriver.ChromeOptions()
    # options.add_argument(
    #     'user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"')
    # driver = webdriver.Chrome()
    # if os.path.exists(r'D:\DataTeam\Products_log\更新时间TXT记录\发行文件.xlsx') == True:
    #     os.remove(r'D:\DataTeam\Products_log\更新时间TXT记录\发行文件.xlsx')
    # gzProd, cmProd = [], []
    # use_selenium()

    # err_fxwj=[gzProd,cmProd]
    # import pickle

    # with open(r'D:\DataTeam\Products_log\更新时间TXT记录\err_fxwj.pkl', 'wb') as f:
    #     pickle.dump(err_fxwj, f)

    # time.sleep(4)
    driver.close()
    driver.quit()
