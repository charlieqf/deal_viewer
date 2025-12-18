from bs4 import BeautifulSoup
import time
import requests
import os
import sys
import io
import ftplib
import pandas as pd
import pymssql
import socket
from datetime import datetime, date, timedelta
from dateutil.parser import parse
import pyodbc
import chardet

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from urllib.parse import urljoin
import traceback
from urllib.parse import quote, urljoin
import threading

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

UPDATE_LOG_PATH = "/Products_log/更新时间TXT记录/发行结果更新时间.txt"
FTP_FOLDER_PATH = "/Products_log/银行间债券市场更新数据"
INCREMENT_FOLDER_PATH = "/增量文档"
DV_FOLDER_PATH = "/DealViewer/TrustAssociatedDoc"
DV_CREDIT_RATING_FOLDER = "ProductCreditRatingFiles"
DV_RELEASE_INSTRUCTION_FOLDER = "ProductReleaseInstructions"
DV_TRUSTEE_REPORT_FOLDER = "TrusteeReport"


def list_ftp_directory_with_retry(ftp, path, retries=5):
    """列出FTP目录内容，带有自动重试和重连功能"""
    last_error = None
    for attempt in range(retries):
        try:
            result = list_ftp_directory(ftp, path)
            if result is not None:  # 即使是空列表也是有效结果
                return result
        except (ftplib.error_temp, ftplib.error_perm, ftplib.error_reply, BrokenPipeError, 
                TimeoutError, ConnectionError, socket.timeout, OSError, Exception) as e:
            last_error = e
            print(f"Error listing directory {path}: {e}. Retrying {retries - attempt - 1} more times.")
            
            # 对于连接相关错误，尝试重新连接FTP
            try:
                print("Connection error detected, attempting to reconnect FTP...")
                # 保存连接信息
                host = ftp.host if hasattr(ftp, 'host') else FTP_HOST
                port = ftp.port if hasattr(ftp, 'port') else FTP_PORT
                
                # 尝试获取登录凭据
                user = None
                passwd = None
                
                if hasattr(ftp, '_user'):
                    user = ftp._user
                elif hasattr(ftp, 'user'):
                    user = ftp.user
                else:
                    user = FTP_USER
                    
                if hasattr(ftp, '_passwd'):
                    passwd = ftp._passwd
                elif hasattr(ftp, 'passwd'):
                    passwd = ftp.passwd
                else:
                    passwd = FTP_PASS
                
                # 尝试关闭旧连接
                try:
                    ftp.close()
                except Exception:
                    pass  # 忽略关闭错误
                
                # 重新连接
                print(f"Reconnecting to {host}:{port} with user {user}...")
                ftp.connect(host, port, timeout=120)
                ftp.login(user, passwd)
                print("FTP reconnection successful")
                
                # 连接成功后，尝试使能UTF8
                try:
                    ftp.sendcmd("OPTS UTF8 ON")
                except Exception as utf8_error:
                    print(f"Warning: Failed to enable UTF8: {utf8_error}")
            except Exception as reconnect_error:
                print(f"Failed to reconnect to FTP: {reconnect_error}")
            
            # 使用指数退避策略
            wait_time = 5 * (2 ** attempt)  # 5, 10, 20, 40...
            if wait_time > 60:
                wait_time = 60  # 最长等待60秒
            print(f"Waiting {wait_time} seconds before next attempt...")
            time.sleep(wait_time)

    # 所有重试失败
    if last_error:
        raise Exception(f"Failed to list directory {path} after {retries} attempts: {last_error}")
    else:
        raise Exception(f"Failed to list directory {path} after {retries} attempts")


def list_ftp_directory(ftp, path):
    """List files and directories in the given FTP path with multiple fallback methods."""
    print(f"Listing directory: {path}")
    
    # 初始化结果
    result = []
    
    # 方法1: 切换目录然后使用NLST
    try:
        # 尝试切换到目标目录
        try:
            ftp.cwd(path)
        except ftplib.error_perm as e:
            # 处理目录不存在的情况
            if "550" in str(e):
                print(f"Directory {path} does not exist")
                return []
            else:
                raise
        
        # 设置更长的超时时间
        original_timeout = None
        if hasattr(ftp, 'sock') and ftp.sock:
            # 保存原始超时
            try:
                original_timeout = ftp.sock.gettimeout()
                ftp.sock.settimeout(180)  # 3分钟超时，更长
            except Exception as e:
                print(f"Warning: Failed to set socket timeout: {e}")
        
        try:
            # 尝试NLST命令
            items = []
            print("Trying NLST command...")
            ftp.retrlines("NLST", items.append)
            result = [item for item in items if item and item.strip()]
            print(f"NLST successful, found {len(result)} items")
        except Exception as e:
            print(f"NLST failed: {e}, trying LIST command instead")
            
            try:
                # 如果NLST失败，尝试LIST命令
                items = []
                ftp.retrlines("LIST", items.append)
                # 从完整的LIST输出中提取文件名
                result = [item.split()[-1] for item in items if item and item.strip()]
                print(f"LIST successful, extracted {len(result)} items")
            except Exception as e2:
                print(f"LIST also failed: {e2}, trying pager fallback")
                
                try:
                    # 尝试使用分页方式获取（减小单次传输量）
                    print("Trying with explicit SIZE command first...")
                    ftp.voidcmd("TYPE I")  # 切换到二进制模式
                    result = []
                    
                    # 尝试使用另一种方式获取当前目录中的文件
                    try:
                        # 先PWD确认当前目录
                        current_dir = ftp.pwd()
                        print(f"Current directory confirmed: {current_dir}")
                        
                        # 使用SITE命令获取目录
                        print("Trying with SITE DIRSTYLE command...")
                        ftp.sendcmd("SITE DIRSTYLE")
                        dir_data = []
                        ftp.dir(dir_data.append)
                        
                        # 解析目录数据
                        for line in dir_data:
                            if "<DIR>" in line:  # 目录
                                parts = line.split()
                                dir_name = parts[-1]
                                if dir_name and dir_name not in [".", ".."]:
                                    result.append(dir_name)
                            elif "." in line:  # 可能是文件
                                parts = line.split()
                                file_name = parts[-1]
                                if file_name:
                                    result.append(file_name)
                    except Exception as e4:
                        print(f"Directory listing with SITE command failed: {e4}")
                except Exception as e3:
                    print(f"All listing methods failed: {e3}")
                    # 此时results仍为空
        finally:
            # 恢复原始超时设置
            if hasattr(ftp, 'sock') and ftp.sock and original_timeout is not None:
                try:
                    ftp.sock.settimeout(original_timeout)
                except Exception as e:
                    print(f"Warning: Failed to restore socket timeout: {e}")
    except Exception as main_e:
        print(f"Major error in list_ftp_directory: {main_e}")
        # 可能需要重新连接FTP，但在这里我们是在retry函数中调用，所以让上层处理重连
    
    # 清理结果，移除空项和特殊项
    result = [item for item in result if item and item.strip() and item not in [".", ".."]]
    print(f"Final result: Found {len(result)} items in {path}")
    return result


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


def keep_alive(ftp, interval):
    """增强的FTP保活线程，带有错误处理和正确响应码识别"""
    while True:
        try:
            # 尝试发送NOOP命令保持连接活跃
            response = ftp.voidcmd("NOOP")
            
            # 正确处理成功响应
            if response.startswith(("1", "2", "3")):
                # 这些是正常的响应代码，不需要特别处理
                if "150" in response or "200 OK" in response:
                    # 这些响应有时会被错误地当做错误，所以特别记录
                    print(f"FTP keep-alive normal response: {response}")
            else:
                # 4xx, 5xx 响应是真正的错误
                print(f"FTP keep-alive received error response: {response}")
        except Exception as e:
            # 检查是否包含常见的成功响应代码
            error_str = str(e)
            if "150 Connection accepted" in error_str or "200 OK" in error_str:
                print(f"Ignoring misleading error with success code: {error_str}")
                # 这不是真正的错误，继续循环
                time.sleep(interval)
                continue
            
            # 捕获所有其他异常，防止线程崩溃
            print(f"FTP keep-alive error: {e}")
            
            # 如果是真正的错误，可能需要重新连接FTP
            try:
                # 检查FTP连接是否仍然活跃
                ftp.voidcmd("PWD")
            except Exception:
                print("FTP connection appears to be broken, attempting reconnect...")
                try:
                    # 尝试重新连接FTP
                    if isinstance(ftp, ftplib.FTP):
                        host = ftp.host
                        port = ftp.port
                        user = ftp._user
                        passwd = ftp._passwd
                        try:
                            ftp.close()
                        except:
                            pass  # 忽略关闭错误
                        ftp.connect(host, port, timeout=600)
                        ftp.login(user, passwd)
                        enable_utf8(ftp)
                        ftp.encoding = "utf-8"
                        print("FTP reconnection successful")
                except Exception as reconnect_error:
                    print(f"Failed to reconnect to FTP: {reconnect_error}")
        
        # 无论是否发生异常，都等待指定的时间
        time.sleep(interval)


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


def keep_alive_backup(ftp, interval):
    """增强的FTP备用保活线程，带有错误处理和正确响应码识别"""
    while True:
        try:
            # 尝试发送NOOP命令保持连接活跃
            response = ftp.voidcmd("NOOP")
            
            # 正确处理成功响应
            if response.startswith(("1", "2", "3")):
                # 这些是正常的响应代码，不需要特别处理
                if "150" in response or "200 OK" in response:
                    # 这些响应有时会被错误地当做错误，所以特别记录
                    print(f"FTP keep-alive backup normal response: {response}")
            else:
                # 4xx, 5xx 响应是真正的错误
                print(f"FTP keep-alive backup received error response: {response}")
        except Exception as e:
            # 检查是否包含常见的成功响应代码
            error_str = str(e)
            if "150 Connection accepted" in error_str or "200 OK" in error_str:
                print(f"Ignoring misleading error with success code: {error_str}")
                # 这不是真正的错误，继续循环
                time.sleep(interval)
                continue
            
            # 捕获所有其他异常，防止线程崩溃
            print(f"FTP keep-alive backup error: {e}")
            
            # 如果是真正的错误，可能需要重新连接FTP
            try:
                # 检查FTP连接是否仍然活跃
                ftp.voidcmd("PWD")
            except Exception:
                print("FTP backup connection appears to be broken, attempting reconnect...")
                try:
                    # 尝试重新连接FTP
                    if isinstance(ftp, ftplib.FTP):
                        host = ftp.host
                        port = ftp.port
                        user = ftp._user
                        passwd = ftp._passwd
                        try:
                            ftp.close()
                        except:
                            pass  # 忽略关闭错误
                        ftp.connect(host, port, timeout=600)
                        ftp.login(user, passwd)
                        enable_utf8(ftp)
                        ftp.encoding = "utf-8"
                        print("FTP backup reconnection successful")
                except Exception as reconnect_error:
                    print(f"Failed to reconnect to FTP backup: {reconnect_error}")
        
        # 无论是否发生异常，都等待指定的时间
        time.sleep(interval)


# Start a thread to keep the connection alive
keep_alive_thread = threading.Thread(
    target=keep_alive_backup, args=(ftp, 60)
)  # NOOP every 5 minutes
keep_alive_thread.daemon = True
keep_alive_thread.start()

keep_alive_thread2 = threading.Thread(
    target=keep_alive_backup, args=(ftp2, 60)
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
#username = "splci64blr"
#password = "6j0z1hrwFM4LbdheZ_"
#proxy_url = f"http://{username}:{password}@gate.smartproxy.com:10001"

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

now_time = str(datetime.now())


def get_pdf_paths_from_html(doc_url, proxies):
    print(f"Fetching detail page: {doc_url}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        }
        response = requests.get(doc_url, headers=headers, proxies=proxies, timeout=30)
        if response.status_code != 200:
            print(f"Failed to fetch detail page. Status: {response.status_code}")
            return []
        
        soup = BeautifulSoup(response.content, "html.parser")
        # Find the file box
        file_box = soup.find("div", class_="allDetailFileBox")
        if not file_box:
            print("Could not find div.allDetailFileBox in detail page.")
            return []
            
        pdf_paths = []
        # Find all links ending in .pdf
        for link in file_box.find_all("a"):
            href = link.get("href")
            text = link.get_text(strip=True)
            if href and href.lower().endswith(".pdf"):
                # Resolve relative URL
                absolute_url = urljoin(doc_url, href)
                pdf_paths.append((absolute_url, text))
                print(f"Found PDF via scrape: {text}")
                
        return pdf_paths

    except Exception as e:
        print(f"Error scraping detail page: {e}")
        traceback.print_exc()
        return []


def use_selenium(proxies):
    # test_url = "https://ip.smartproxy.com/json"

    # response = requests.get(test_url, proxies=proxies)
    # print(response.status_code, response.json())

    print("connecting to the website...")
    driver.get(
        "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/"
    )
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    time.sleep(3)  # Wait for the page to load

    print(driver.title)

    # # Check if the element exists on the page
    # elements = driver.find_elements(By.XPATH, "//span[contains(text(),'发行结果')]")
    # if len(elements) > 0:
    #     print("Element found on the page")
    # else:
    #     print("Element not found on the page")

    # # Print the page source for debugging
    # page_source = driver.page_source
    # print("Page source:", page_source)

    # # Click on the "发行结果" element
    # try:
    #     print("Clicking on the '发行结果' element...")
    #     WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(),'发行结果')]"))).click()
    #     time.sleep(3)
    # except Exception as e:
    #     print("Error clicking on the element: ", e)

    last_date = read_ftp_file(ftp, UPDATE_LOG_PATH)
    print("上次更新的日期为 " + last_date)

    url = "https://www.chinabond.com.cn/cbiw/trs/getContentByConditions"
    data = {
            "parentChnlName": "zqzl_zjzzczj",
            "excludeChnlNames": [],
            "childChnlDesc": "发行结果",
            "hasAppendix": True,
            "siteName": "chinaBond",
            "pageSize": 50,
            "pageNum": 1,
            "queryParam": {
                "keywords": "",
                "startDate": "",
                "endDate": "",
                "reportType": "",
                "reportYear": "",
                "ratingAgency": ""
            }
        }

    # Define headers for the request
    # Define headers for the request
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        "Origin": "https://www.chinabond.com.cn",
        "Referer": "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/",
    }

    # Configure the proxy
    proxies = {
        "http": proxy_url,
        "https": proxy_url,
    }

    # Send the POST request
    # Send the POST request
    response = requests.post(url, json=data, headers=headers, proxies=proxies)
    # print(response.status_code, response.reason, response.text, response.headers)
    response_data = response.json()

    products = []
    latest_date_time = last_date  # Initialize with the last date in case no new data is found
    if response_data["success"]:
        list_data = response_data["data"]["list"]
        latest_date_time = max(
            [parse(item["shengXiaoShiJian"]) for item in list_data]
        ).strftime("%Y-%m-%d %H:%M:%S")
        print(list_data)  # Inspect the structure of the list_data
        for item in list_data:
            """
            New Structure:
            {
                "docContent": 0,
                "shengXiaoShiJian": "2024-06-04 10:05:37",
                "docTitle": "工元至诚2024年第一期不良资产支持证券簿记建档发行结果公告",
                "docid": 853810682,
                "docPubUrl": "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/zjzczq_ABS/ABS_fxjg_ath/202406/t20240604_853810682.html",
                "appendixIds": "1415317=P020240604363380159323.pdf=工元至诚2024年第一期不良资产支持证券簿记建档发行结果公告.pdf"
            }
            """
            issue_time = item.get("shengXiaoShiJian", "")
            doc_title = item.get("docTitle", "")
            doc_url = item.get("docPubUrl", "")
            appendix_ids = item.get("appendixIds", "")

            pdf_path_home = doc_url.rsplit("/", 1)[0]
            
            # Use scraper if appendix_ids is missing
            if appendix_ids:
                 try:
                    pdf_name = appendix_ids.split('=')[1]
                    pdf_path = f"{pdf_path_home}/{pdf_name}"
                 except:
                    print(f"Failed to parse appendixIds: {appendix_ids}")
                    continue
            else:
                 print(f"No appendixIds for {doc_title}, attempting to scrape detail page...")
                 found_pdfs = get_pdf_paths_from_html(doc_url, proxies)
                 if found_pdfs:
                     pdf_path = found_pdfs[0][0]
                 else:
                     print(f"No PDFs found for {doc_title}")
                     continue

            # save the data to products

            if parse(issue_time) > parse(last_date):
                products.append(
                    {
                        "title": doc_title,
                        "issue_time": issue_time,
                        "pdf_path": pdf_path,
                    }
                )

    # Process the products
    print("Processing products... (total: %d)" % len(products))
    # print(products)
    update_pdf_new(products)

    print("Writing latest date time {} to".format(latest_date_time), UPDATE_LOG_PATH)
    with io.BytesIO(latest_date_time.encode("utf-8")) as bio:
        ftp.storbinary(f"STOR {UPDATE_LOG_PATH}", bio)

def get_web_pdf_content_with_retry(web_pdf_path, retries=5):
    """下载PDF内容，有指数退避重试和隔离错误处理"""
    
    last_error = None
    for attempt in range(retries):
        try:
            success, content = get_web_pdf_content(web_pdf_path)
            if success:
                return success, content
            else:
                # 如果获取失败但不是由于异常，记录失败原因
                last_error = content
                print(f"Download attempt {attempt+1} failed: {content}")
        except Exception as e:
            last_error = str(e)
            print(
                f"Error occurred while getting PDF content from {web_pdf_path}: {e}. "
                f"Retrying {retries - attempt - 1} more times."
            )
        
        # 使用指数退避策略
        wait_time = 2 * (2 ** attempt)  # 2, 4, 8, 16, 32...
        if wait_time > 60:
            wait_time = 60  # 最长等待60秒
        print(f"Waiting {wait_time} seconds before next attempt...")
        time.sleep(wait_time)

    # 如果之前有错误记录，返回该错误
    if last_error:
        return False, last_error
    else:
        raise Exception(f"Failed to get PDF content from {web_pdf_path} after {retries} attempts")

def get_web_pdf_content(web_pdf_path):
    """获取网站PDF文件内容，先尝试直接下载，如果因IP限制失败则使用代理"""
    
    encoded_url = quote(web_pdf_path, safe=":/")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/pdf, */*",
        "Connection": "keep-alive",
        "Referer": "/".join(web_pdf_path.split("/")[:3]),
    }
    
    # 先尝试不使用代理直接下载
    print(f"Attempting to download without proxy first: {encoded_url}")
    try:
        # 使用session来管理连接
        session = requests.Session()
        # 设置连接池参数
        adapter = requests.adapters.HTTPAdapter(
            max_retries=3,
            pool_connections=5,
            pool_maxsize=10
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        response = session.get(
            encoded_url, 
            headers=headers, 
            timeout=(15, 45),  # 增加超时时间
            stream=True  # 流式下载减少内存占用
        )
        
        # 检查响应成功
        if response.status_code == 200 and response.headers.get("Content-Type", "").lower().startswith(("application/pdf", "binary/octet-stream")):
            print(f"Download successful without proxy: {response.status_code}, {response.headers.get('Content-Type')}")
            # 获取全部内容
            content = response.content
            return True, content
        
        # 检查是否为IP相关的失败
        ip_related_failure = False
        if response.status_code in [403, 429, 451, 503]:
            ip_related_failure = True
            print(f"Status code {response.status_code} indicates possible IP restriction")
        elif response.status_code != 200:
            # 检查响应文本中的IP限制消息
            ip_block_indicators = ["blocked", "forbidden", "access denied", "IP", "地址被禁止", "访问受限", "访问被拒绝", "rate limit", "too many requests"]
            try:
                # 限制读取4K文本以检查错误消息
                response_text = response.text[:4096].lower()
                for indicator in ip_block_indicators:
                    if indicator.lower() in response_text:
                        ip_related_failure = True
                        print(f"Found blocking indicator: {indicator}")
                        break
            except Exception as text_error:
                print(f"Error reading response text: {text_error}")
        
        if not ip_related_failure:
            # 失败但不是由于IP限制
            print(f"Failed to download without proxy. Status: {response.status_code}. Not IP related.")
            return False, f"HTTP Error: {response.status_code} - {response.reason}"
            
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error during non-proxy download: {e}")
        # 连接被拒绝时尽快转为使用代理
        ip_related_failure = True
    except requests.exceptions.Timeout as e:
        print(f"Timeout during non-proxy download: {e}")
        ip_related_failure = True
    except Exception as e:
        print(f"Error during non-proxy download attempt: {e}")
        # 对于其他类型的错误，假设可能是IP相关
        ip_related_failure = True
    
    # 如果有IP限制或发生异常，使用SmartProxy重试
    if ip_related_failure:
        print("IP restriction detected or connection error, trying download with SmartProxy...")
        try:
            # 新建一个session专门用于代理连接
            proxy_session = requests.Session()
            proxy_adapter = requests.adapters.HTTPAdapter(
                max_retries=3,
                pool_connections=5,
                pool_maxsize=10
            )
            proxy_session.mount('http://', proxy_adapter)
            proxy_session.mount('https://', proxy_adapter)
            
            # 使用代理的请求
            response = proxy_session.get(
                encoded_url, 
                headers=headers, 
                proxies=proxies, 
                timeout=(20, 60),  # 代理请求可能需要更长的超时
                stream=True
            )
            
            # 检查内容是否为PDF
            print(f"Proxy response status: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}")
            content_type = response.headers.get("Content-Type", "").lower()
            if response.status_code == 200 and (content_type.startswith(("application/pdf", "binary/octet-stream"))):
                print("Successfully downloaded with proxy")
                content = response.content
                return True, content
            else:
                error_msg = f"Failed with proxy. Status: {response.status_code}, Content-Type: {content_type}"
                print(error_msg)
                # 获取错误响应的前1KB以识别问题
                try:
                    error_content = response.content[:1024]
                    return False, f"{error_msg}\nResponse preview: {error_content}"
                except:
                    return False, error_msg
                
        except requests.exceptions.RequestException as e:
            error_msg = f"RequestException during proxy download: {e}"
            print(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error during proxy download: {e}"
            print(error_msg)
            return False, error_msg
    
    # 如果我们没有使用代理也没有成功返回
    return False, "Failed to download with both direct connection and proxy"


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
    cache_folder = os.path.join(current_dir, "fxjg_file_cache")
    if not os.path.exists(cache_folder):
        os.makedirs(cache_folder)

    asset_type_dirs = [
        "/Products/不良资产",
        "/Products/车贷",
        "/Products/房贷",
        "/Products/企业贷款",
        "/Products/小微贷款",
        "/Products/消费贷",
        "/Products/信用卡",
        "/Products/租赁资产",
        "/Products_log/银行间债券市场更新数据",
    ]

    for asset_type_dir in asset_type_dirs:
        print(asset_type_dir)
        folders = list_ftp_directory_with_retry(ftp, asset_type_dir)
        for product_folder in folders:
            # if product_folder is not a folder, skip
            if "." in product_folder:
                continue

            product_folder_path = os.path.join(asset_type_dir, product_folder)
            # print(f"Processing: {item_path}")
            product_keyword = product_folder.split("资产")[0]
            if len(product_keyword) < 4:
                continue

            #print("Scanning:", product_folder_path, "keyword =", product_keyword)
            # find the product whose title matches the key_words
            for product in products:
                if product_keyword in product["title"]:
                    print(f"Matched: {product_keyword} with {product['title']}")
                    # download the pdf file
                    pdf_file_name = f"{product['title']}.pdf"
                    ftp_pdf_path = os.path.join(product_folder_path, pdf_file_name)
                    web_pdf_path = product["pdf_path"]

                    # check if file exists in the cache folder; download if not
                    cache_pdf_path = os.path.join(cache_folder, pdf_file_name)
                    file_exists = os.path.exists(cache_pdf_path)

                    if not file_exists:
                        print(
                            "Downloading PDF from",
                            web_pdf_path,
                            "to cache folder ====>",
                        )
                        try:
                            result, conctent = get_web_pdf_content_with_retry(web_pdf_path)
                            if result:
                                with open(cache_pdf_path, "wb") as f:
                                    f.write(conctent)
                                file_exists = True

                        except Exception as e:
                            print("Error downloading PDF file:", e)
                    else:
                        print("PDF file already exists in cache folder")

                    if file_exists:
                        print("Uploading PDF to", ftp_pdf_path)
                        upload_file_to_ftp(
                            ftp,
                            cache_pdf_path,
                            product_folder_path,
                            ftp_pdf_path,
                            pdf_file_name,
                        )

                        # 上传到增量文档 INCREMENT_FOLDER_PATH
                        increment_foler_path = create_dir_on_ftp(ftp, month_folder_path, product_folder)
                        increment_pdf_path = create_dir_on_ftp(ftp, increment_foler_path, pdf_file_name)

                        print("Uploading PDF to", increment_pdf_path)
                        upload_file_to_ftp(
                            ftp,
                            cache_pdf_path,
                            increment_foler_path,
                            increment_pdf_path,
                            pdf_file_name,
                        )

                        # look for trust_code.txt in the product folder on the FTP server
                        files = list_ftp_directory_with_retry(ftp, product_folder_path)

                        for file in files:
                            if ".txt" in file:
                                trust_code = file.split(".")[0]

                                ftp_folder_path = create_dir_on_ftp(
                                    ftp2, DV_FOLDER_PATH, trust_code
                                )
                                ftp_folder_path = create_dir_on_ftp(
                                    ftp2, ftp_folder_path, DV_TRUSTEE_REPORT_FOLDER
                                )
                                ftp_pdf_path = os.path.join(ftp_folder_path, pdf_file_name)
                                
                                print("Uploading PDF to", ftp_pdf_path)
                                upload_file_to_ftp(
                                    ftp2,
                                    cache_pdf_path,
                                    ftp_folder_path,
                                    ftp_pdf_path,
                                    pdf_file_name,
                                )

                                sql_file_path = 'DealViewer/TrustAssociatedDoc/' + trust_code + '/TrusteeReport/'

                                insert_db_record(
                                    trust_code, sql_file_path, pdf_file_name
                                )

    print("Done")


def update_pdf():
    with open(
        r"D:\DataTeam\Products_log\更新时间TXT记录\发行结果更新时间.txt", "r"
    ) as f:
        last_date = f.read()
        print("上次更新的日期为" + last_date)
    f.close()

    soup = BeautifulSoup(driver.page_source, "lxml")
    update_times = soup.select("#list_list > li > span:nth-of-type(1)")
    update_contents = soup.select("span.unlock > a")

    # first_update_times = soup.select('#list_fxwj > li > span:nth-of-type(1)')[0].text.split(' ')[0]
    # print("这次更新的日期为"+first_update_times+"，将会写到pdf_time.txt中")
    # \\172.16.7.168\Shared\AnalysisFramework\资产证券化数据\信贷资产证券化\银行间债券市场

    file_dirs = [
        r"\\172.16.7.114\DataTeam\Products\不良资产",
        r"\\172.16.7.114\DataTeam\Products\车贷",
        r"\\172.16.7.114\DataTeam\Products\房贷",
        r"\\172.16.7.114\DataTeam\Products\企业贷款",
        r"\\172.16.7.114\DataTeam\Products\小微贷款",
        r"\\172.16.7.114\DataTeam\Products\消费贷",
        r"\\172.16.7.114\DataTeam\Products\信用卡",
        r"\\172.16.7.114\DataTeam\Products\租赁资产",
        r"\\172.16.7.114\DataTeam\Products_log\银行间债券市场更新数据",
    ]

    for update_time, update_content in zip(update_times, update_contents):
        data = {
            "update_time": update_time,
            "update_link": update_content.get("href"),
            "update_title": update_content.get("title"),
        }
        web_date = data.get("update_time").text

        if web_date > last_date:
            for file_dir in file_dirs:
                for dirs in os.listdir(file_dir):
                    if str(dirs).split("资产")[0] in data["update_title"]:
                        pathname = os.path.join(file_dir, dirs)
                        filename = data["update_title"] + ".pdf"
                        # 遍历本地的产品，匹配到网络的，如果匹配到，则下载该产品目录下，即便了出现了关于也不影响啦啦
                        # while data['title'] in str(dirs).split('化')[0]:
                        print(data["update_title"] + "与" + dirs + "匹配")
                        headers = {
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"
                        }
                        try:
                            print(data.get("update_link"))
                            print(data["update_title"])
                            rep = requests.get(data.get("update_link"), headers=headers)
                            time.sleep(2)
                            print(
                                file_dir
                                + r"\{}".format(dirs)
                                + r"\{}.pdf".format(data["update_title"])
                            )
                            with open(
                                file_dir
                                + r"\{}".format(dirs)
                                + r"\{}.pdf".format(data["update_title"]),
                                "wb",
                            ) as f:
                                f.write(rep.content)
                                f.close()

                            # 增量文档
                            zlpath = r"\\172.16.7.114\DataTeam\增量文档"
                            month = datetime.now().month
                            monthpath = os.path.join(zlpath, str(month))
                            cppath = os.path.join(monthpath, dirs)
                            if os.path.exists(monthpath) == True:

                                cppath = os.path.join(monthpath, dirs)

                                if os.path.exists(cppath) == True:
                                    with open(
                                        cppath + r"\{}".format(data["update_title"]),
                                        "wb",
                                    ) as f:
                                        f.write(rep.content)
                                        f.close()
                                else:
                                    os.mkdir(cppath)
                                    with open(
                                        cppath + r"\{}".format(data["update_title"]),
                                        "wb",
                                    ) as f:
                                        f.write(rep.content)
                                        f.close()
                            else:
                                os.mkdir(monthpath)
                                os.mkdir(cppath)
                                with open(
                                    cppath + r"\{}".format(data["update_title"]), "wb"
                                ) as f:
                                    f.write(rep.content)
                                    f.close()

                        except Exception as e:
                            print(e)

                        print(data["update_title"] + "下载完成")
                        with open(
                            "D:\DataTeam\Products_log\更新时间TXT记录\FileName.txt", "a"
                        ) as f:
                            f.write(now_time)
                            f.write(data["update_title"])
                            f.write("\n")
                            f.close()

                        with open(
                            r"D:\DataTeam\Products_log\更新时间TXT记录\发行结果报告.xlsx",
                            "a",
                        ) as f:
                            f.write(now_time)
                            f.write(data["update_title"])
                            f.write("\n")
                            f.close()

                        ftp_path = "DealViewer\TrustAssociatedDoc"
                        folder = "TrusteeReport"
                        for file in os.listdir(pathname):
                            if ".txt" in file:
                                trust_code = file.split(".")[0]
                                ftp_path1 = os.path.join(ftp_path, trust_code)
                                ftp_path2 = os.path.join(ftp_path1, folder)
                                sqlfilepath = ftp_path2 + "/"
                                sqlfilepath = sqlfilepath.replace("\\", "/")

                                remotepath = "./" + filename
                                localpath = os.path.join(pathname, filename)
                                ftp_file = "DealViewer\TrustAssociatedDoc{}".format(
                                    trust_code
                                )
                                upload_file(
                                    remotepath,
                                    localpath,
                                    ftp_path2,
                                    trust_code,
                                    sqlfilepath,
                                    filename,
                                    ftp_file,
                                )

                                insert_db_record(
                                    trust_code, sqlfilepath, filename, web_date
                                )
        else:
            record_latest_time()
            print("程序执行完毕")
            driver.close()
            sys.exit(0)


def record_latest_time():
    # 选择下拉框回到第一页，获取最新的日期
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//select[@id='sel']/option[1]"))
    )
    element.click()
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "lxml")
    first_update_times = soup.select("#list_list > li > span:nth-of-type(1)")[0].text
    with open(
        r"D:\DataTeam\Products_log\更新时间TXT记录\发行结果更新时间.txt", "w"
    ) as f:
        f.write(first_update_times)
    f.close()


# 上传FTP
def upload_file(
    remotepath, localpath, ftp_path2, trust_code, sqlfilepath, filename, ftp_file
):
    ftp = FTP()
    host = "192.168.1.211"
    port = 21
    username = "gsuser"
    password = "Password01"
    ftp.connect(host, 21)
    ftp.login(username, password)
    try:
        ftp.mkd(ftp_path2)
        print("产品文件新建完成")
        # ftp.mkd(ftp_path2)

        ftp.cwd(ftp_path2)
        bufsize = 1024
        ftp.encoding = "GBK"
        fp = open(localpath, "rb")
        ftp.storbinary("STOR " + remotepath, fp, bufsize)
        print("文件上传FTP成功")
        ftp.set_debuglevel(0)
        fp.close()
    except:
        print("产品文件新建失败")
        try:
            ftp.mkd(ftp_path2)
            print("FTP文件夹存在，已切换")
            bufsize = 1024
            ftp.encoding = "GBK"
            fp = open(localpath, "rb")
            ftp.storbinary("STOR " + remotepath, fp, bufsize)
            print("文件上传FTP成功")
            ftp.set_debuglevel(0)
            fp.close()
            insert_db_record(trust_code, sqlfilepath, filename)
        except:
            print("类型文件存在")
            try:
                ftp.cwd(ftp_path2)
                print("FTP文件夹存在，已切换")
                bufsize = 1024
                ftp.encoding = "GBK"
                fp = open(localpath, "rb")
                ftp.storbinary("STOR " + remotepath, fp, bufsize)
                print("文件上传FTP成功")
                ftp.set_debuglevel(0)
                fp.close()
                insert_db_record(trust_code, sqlfilepath, filename)
            except:
                print("上传产品文件失败")
                pass


# TrustAssociatedDocument表插入记录(DV页面显示)
def insert_db_record(trust_code, sqlfilepath, filename):
    b1 = conn.cursor()
    try:
        sql = "select Trustid from DV.view_Products where TrustCode='{}'".format(
            trust_code
        )
        b1.execute(sql)
        trust_id = b1.fetchone()[0]

        sql = f"select count(1) from DV.TrustAssociatedDocument where Trustid={trust_id} and FileName=N'{filename}'"
        b1.execute(sql)

        if b1.fetchone()[0] == 0:
            sql = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values({},'AnnouncementOfResults','NULL','{}',N'{}','pdf',GETDATE(),'py')".format(
                trust_id, sqlfilepath, filename
            )

            b1.execute(sql)
            conn.commit()
            print("记录插入成功")
    except:
        print(filename, "记录插入失败!")


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

    driver.close()
    driver.quit()
