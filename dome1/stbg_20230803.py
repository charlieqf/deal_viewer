import smtplib
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import requests
import os
import io
import sys
import ftplib
import pymssql
from datetime import datetime, date, timedelta
from dateutil.parser import parse
from urllib.parse import quote
from email.mime.text import MIMEText
import pyodbc
import chardet
import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType

error_flag = False

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

UPDATE_LOG_PATH = "/Products_log/更新时间TXT记录/受托报告更新时间.txt"
FTP_FOLDER_PATH = "/Products_log/银行间债券市场更新数据"
INCREMENT_FOLDER_PATH = "/增量文档"
DV_FOLDER_PATH = "/DealViewer/TrustAssociatedDoc"
DV_CREDIT_RATING_FOLDER = "ProductCreditRatingFiles"
DV_RELEASE_INSTRUCTION_FOLDER = "ProductReleaseInstructions"
DV_TRUSTEE_REPORT_FOLDER = "TrusteeReport"


def list_ftp_directory_with_retry(ftp, path, retries=5):
    for attempt in range(retries):
        try:
            return list_ftp_directory(ftp, path)
        except (ftplib.error_temp, ftplib.error_perm, BrokenPipeError) as e:
            print(
                f"Temporary error occurred: {e}. Retrying {retries - attempt - 1} more times."
            )
            time.sleep(5)  # Wait for 5 seconds before retrying

    error_flag = True
    raise Exception("Failed to list directory after multiple attempts")


def list_ftp_directory(ftp, path):
    """List files and directories in the given FTP path."""

    ftp.cwd(path)
    raw_items = []

    # Use retrbinary to get raw binary data
    ftp.retrbinary("NLST", raw_items.append)

    # Join the raw binary data and detect encoding
    raw_data = b"".join(raw_items)
    detected_encoding = chardet.detect(raw_data)["encoding"]
    if detected_encoding is None:
        detected_encoding = "utf-8"  # Fallback to utf-8 if detection fails
    #     print("Encoding detection failed, using utf-8 as fallback")
    # else:
    #     print(f"Detected encoding: {detected_encoding}")

    try:
        # Attempt to decode with detected encoding
        items = raw_data.decode(detected_encoding).split("\r\n")
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


def keep_alive(ftp, interval):
    while True:
        ftp.voidcmd("NOOP")
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


def keep_alive(ftp, interval):
    while True:
        ftp.voidcmd("NOOP")
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
username = "splci64blr"
password = "6j0z1hrwFM4LbdheZ_"
proxy_url = f"http://{username}:{password}@gate.smartproxy.com:10001"
proxies = {
    "http": proxy_url,
    "https": proxy_url,
}

now_time = str(datetime.now())


def use_selenium(proxies):
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

    last_date = read_ftp_file(ftp, UPDATE_LOG_PATH)
    print("上次更新的日期为 " + last_date)

    url = "https://www.chinabond.com.cn/cbiw/trs/getDocsByConditions"
    data = {
        "childChnlName": "付息兑付与行权公告",
        "keywords": "",
        "pageNum": 1,
        "isHasAppendix": 1,
        "pageSize": 100,
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
    if response_data["success"]:
        list_data = response_data["data"]["data"]["list"]
        latest_date_time = max(
            [parse(item["ShengXiaoShiJian"]) for item in list_data]
        ).strftime("%Y-%m-%d %H:%M:%S")

        print(list_data)  # Inspect the structure of the list_data
        for item in list_data:
            """
            {
                "DOCCONTENT": "",
                "ShengXiaoShiJian": "2024-06-03 08:29:41",
                "DocTitle": "兴晴2023年第一期个人消费贷款资产支持证券受托机构报告（第十一期）",
                "docid": 853810060,
                "FaXingQiShu": "null",
                "DOCPUBURL": "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/zjzczq_ABS/ABS_fxdfyxqgg/202406/t20240603_853810060.html",
                "MetaDataId": 853810060,
                "OriginDocId": 853810060,
                "recid": 1046394,
                "appendixIds": "1414775=P020240603305814563350.pdf=兴晴2023年第一期个人消费贷款资产支持证券受托机构报告（第十一期）.pdf",
                "FaXingNianFen": "2023"
            }

            pdf path = https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/zjzczq_ABS/ABS_fxdfyxqgg/202406/P020240603305814563350.pdf
            pdf_path = DOCPUBURL remove the bits after the last /, then add part of appendixIds
            """
            issue_time = item.get("ShengXiaoShiJian", "")
            doc_title = item.get("DocTitle", "")
            doc_url = item.get("DOCPUBURL", "")
            appendix_ids = item.get("appendixIds", "")

            pdf_path_home = doc_url.rsplit("/", 1)[0]
            pdf_path = f"{pdf_path_home}/{appendix_ids.split('=')[1]}"

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

    if error_flag == False:
        print("Writing latest date time {} to".format(latest_date_time), UPDATE_LOG_PATH)
        with io.BytesIO(latest_date_time.encode("utf-8")) as bio:
            ftp.storbinary(f"STOR {UPDATE_LOG_PATH}", bio)


def get_web_pdf_content_with_retry(web_pdf_path, retries=5):
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


def update_pdf_new(products):
    month = str(datetime.now().month)
    month_folder_path = create_dir_on_ftp(ftp, INCREMENT_FOLDER_PATH, month)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    cache_folder = os.path.join(current_dir, "stbg_file_cache")
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
                error_file_path = os.path.join(cache_folder, f"{product['title']}.error")
                success_file_path = os.path.join(cache_folder, f"{product['title']}.success")

                # if 'product_title.success' in cache folder, skip
                if os.path.exists(success_file_path):
                    #print(product["title"], "already processed")
                    continue

                web_date = product["issue_time"]
                if product_keyword in product["title"]:
                    print(f"Matched: {product_keyword} with {product['title']}")
                    try:
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

                                    sql_folder_path = ftp_folder_path + '/'
                                    
                                    print("Uploading PDF to", ftp_pdf_path)
                                    upload_file_to_ftp(
                                        ftp2,
                                        cache_pdf_path,
                                        ftp_folder_path,
                                        ftp_pdf_path,
                                        pdf_file_name,
                                    )

                                    insert_db_record(
                                        trust_code, sql_folder_path, pdf_file_name
                                    )

                                    try:
                                        update_max_period_number(trust_code)
                                    except:
                                        print(trust_code, '期数更新失败')
                                        if not os.path.exists(error_file_path):
                                            with open(error_file_path, "w") as f:
                                                f.write(str(e))

                                    try:
                                        insert_information(trust_code, pdf_file_name, web_date)
                                        # create a 'product_title.success' file in the cache folder
                                        with open(success_file_path, "w") as f:
                                            f.write("Success")

                                        # delete .error file if exists
                                        if os.path.exists(error_file_path):
                                            os.remove(error_file_path)

                                    except:
                                        print(trust_code, '披露信息插入失败!')
                                        if not os.path.exists(error_file_path):
                                            with open(error_file_path, "w") as f:
                                                f.write(str(e))

                    except Exception as e:
                        #print(e)
                        # create a 'product_title.error' file in the cache folder
                        if not os.path.exists(error_file_path):
                            with open(error_file_path, "w") as f:
                                f.write(str(e))
                            print("Error occurred while processing", product["title"], e)

                            
    print("Done")




def date():
    # 选择下拉框回到第一页，获取最新的日期
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH , "//select[@id='sel']/option[1]"))
    )
    element.click()
    time.sleep(10)

    soup = BeautifulSoup(driver.page_source, 'lxml')
    first_update_times = soup.select('#list_list > li > span:nth-of-type(1)')[0].text
    with open(r'D:\DataTeam\Products_log\更新时间TXT记录\受托报告更新时间.txt', 'w') as f:
        f.write(first_update_times)
    f.close()


def update_pdf():
    with open(r'D:\DataTeam\Products_log\更新时间TXT记录\受托报告更新时间.txt', 'r') as f:
        last_date = f.read()
        print("上次更新的日期为" + last_date)
    f.close()
    soup = BeautifulSoup(driver.page_source, 'lxml')
    update_times = soup.select('#list_list > li > span:nth-of-type(1)')
    update_links = soup.select('span.unlock > a')
    update_titles = soup.select('span.unlock > a')

    for update_time, update_link, update_title in zip(update_times, update_links, update_titles):

        data = {
            'update_time': update_time,
            'update_link': update_link.get('href'),
            'update_title': update_title.get('title')
        }
        web_date = data.get('update_time').text
        file_name = data['update_title']

        # if web_date > '2024-05-10 11:30':
        #     continue
        if web_date > last_date:
            print(web_date)
            file_dirs = [
                r"\\172.16.7.114\DataTeam\Products\不良资产",
                r"\\172.16.7.114\DataTeam\Products\车贷",
                r"\\172.16.7.114\DataTeam\Products\房贷",
                r"\\172.16.7.114\DataTeam\Products\企业贷款",
                r"\\172.16.7.114\DataTeam\Products\小微贷款",
                r"\\172.16.7.114\DataTeam\Products\消费贷",
                r"\\172.16.7.114\DataTeam\Products\信用卡",
                r"\\172.16.7.114\DataTeam\Products\租赁资产",
                r"\\172.16.7.114\DataTeam\Products\金融租赁",
                r"\\172.16.7.114\DataTeam\Products_log\银行间债券市场更新数据"
            ]
            found = 0
            for file_dir in file_dirs:
                if found == 1:
                    break
                for dirs in os.listdir(file_dir):
                    if str(dirs).split('资产')[0] in data['update_title']:
                        found = 1
                        pathname = os.path.join(file_dir, dirs)
                        filename = data['update_title'] + '.pdf'
                        print(data['update_title'] + "与" + dirs + "匹配")
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
                        try:
                            if '更正' in data['update_title'] or '以此为准' in data['update_title']:
                                rep = requests.get(data.get('update_link'), headers=headers)
                                soup = BeautifulSoup(rep.text, 'lxml')
                                for a in soup.find_all('a', href=True, title=True):
                                    if '关于' not in a.get('title'):
                                        rep = requests.get(a.get('href'), headers=headers)
                                        break
                            else:
                                rep = requests.get(data.get('update_link'), headers=headers)
                            time.sleep(4)
                            with open(file_dir + r'\{}'.format(dirs) + r'\{}.pdf'.format(data['update_title']),
                                      'wb') as f:
                                f.write(rep.content)

                                # 增量文档
                            zlpath = r'\\172.16.7.114\DataTeam\增量文档'
                            month = datetime.now().month
                            monthpath = os.path.join(zlpath, str(month))
                            cppath = os.path.join(monthpath, dirs)
                            if os.path.exists(monthpath) == True:
                                cppath = os.path.join(monthpath, dirs)
                                if os.path.exists(cppath) == True:
                                    with open(cppath + r'\{}'.format(data['update_title']), 'wb') as f:
                                        f.write(rep.content)
                                        f.close()
                                else:
                                    os.mkdir(cppath)
                                    with open(cppath + r'\{}'.format(data['update_title']), 'wb') as f:
                                        f.write(rep.content)
                                        f.close()
                            else:
                                os.mkdir(monthpath)
                                os.mkdir(cppath)
                                with open(cppath + r'\{}'.format(data['update_title']), 'wb') as f:
                                    f.write(rep.content)
                                    f.close()

                        except Exception as e:
                            print(e)

                        print(data['update_title'] + "下载完成")
                        with open('D:\DataTeam\Products_log\更新时间TXT记录\FileName.txt', 'a') as f:
                            f.write(now_time)
                            f.write(data['update_title'])
                            f.write('\n')
                            f.close()

                        with open(r'D:\DataTeam\Products_log\更新时间TXT记录\更新受托报告.xlsx', 'a') as f:
                            f.write(now_time)
                            f.write(data['update_title'])
                            f.write('\n')
                            f.close()

                        ftp_path = 'DealViewer\TrustAssociatedDoc'
                        folder = 'TrusteeReport'
                        for file in os.listdir(pathname):
                            if '.txt' in file:
                                trust_code = file.split('.')[0]
                                # print(TrustCode)
                                ftp_path1 = os.path.join(ftp_path, trust_code)
                                # print(ftp_path1)
                                ftp_path2 = os.path.join(ftp_path1, folder)
                                # print(ftp_path2)
                                sqlfilepath = ftp_path2 + '/'
                                sqlfilepath = sqlfilepath.replace('\\', '/')

                                remotepath = './' + filename
                                localpath = os.path.join(pathname, filename)
                                ftp_file = 'DealViewer\TrustAssociatedDoc{}'.format(trust_code)
                                upload_file(remotepath, localpath, ftp_path2, trust_code, sqlfilepath, filename,
                                            ftp_file)
                                # print(remotepath,localpath,ftp_path2,TrustCode,sqlfilepath,filename,ftp_file)

                                insert(trust_code, sqlfilepath, filename, web_date)
                                try:
                                    update_max_period_number(trust_code)
                                except:
                                    print(trust_code, '期数更新失败')
                                    pass
                                try:
                                    insert_information(trust_code, file_name, web_date)
                                except:
                                    print(trust_code, '披露信息插入失败!')
                                    pass
                        break
            if found == 0:
                notFound.append(data['update_title'])

        else:
            date()
            err_stbg=[mailFiles,notFound]
            import pickle

            with open(r'D:\DataTeam\Products_log\更新时间TXT记录\err_stbg.pkl', 'wb') as f:
                pickle.dump(err_stbg, f)
            sys.exit(0)


# 更新最大期数
def update_max_period_number(trust_code):
    b2 = conn.cursor()

    sql = "select TrustId from TrustManagement.Trust where TrustCode='{}'".format(trust_code)
    print(sql)
    b2.execute(sql)
    trust_id = b2.fetchone()[0]
    print("trust_id =", trust_id)

    sql = "select TrustNameShort from TrustManagement.Trust where TrustId={}".format(trust_id)
    b2.execute(sql)
    trust_name_short = b2.fetchone()[0]
    print(trust_name_short)

    # 判断最大期数表中是否存在该产品
    sql = "select TrustId from dbo.ReportMaxNper"
    b2.execute(sql)
    max_period_id = b2.fetchall()
    conn.commit()

    MaxNperId = []
    for NperId in max_period_id:
        MaxNperId.append(NperId[0])
    if trust_id in MaxNperId:
        sql = "update dbo.ReportMaxNper set MaxNper += 1 where TrustId ={}".format(trust_id)
        print(sql)
        b2.execute(sql)
        print('期数更新完成!')
    else:
        sql = "insert into dbo.ReportMaxNper values({},N'{}',1,N'{}')".format(trust_id, trust_name_short, '存续期')
        print(sql)
        b2.execute(sql)
        conn.commit()

    sql = "select MaxNper from dbo.ReportMaxNper where TrustId={}".format(trust_id)
    print(sql)
    b2.execute(sql)
    MaxNper = b2.fetchone()[0]
    conn.commit()
    print('最大期数为{}更新成功！'.format(MaxNper))


# 爬取披露信息插入（任务分配系统显示） ++状态信息
def insert_information(trust_code, file_name, web_date):
    b1 = conn.cursor()
    sql = "select TrustId from TrustManagement.Trust where TrustCode='{}'".format(trust_code)
    b1.execute(sql)
    trust_id = b1.fetchone()[0]
    if trust_id in(10804,10759):
        print('10804,10759不上任务系统')
        return
    
    web_date = str(web_date)

    web_date = parse(str(web_date))
    sql = "select max(DisclosureTime) from dbo.DisclosureOfInformation where TrustId={}".format(
        trust_id)
    b1.execute(sql)
    SDisclosureTimeMax = b1.fetchone()[0]

    web_date = parse(str(web_date))
    SDisclosureTimeMax = parse(str(SDisclosureTimeMax))
    cz = (web_date - SDisclosureTimeMax).days
    if cz < 17 or '更正' in file_name:
        print('文件披露可能重复!')
    elif '半年' in file_name:
        print('半年受托报告不需要披露')
    else:
        if '清算' in file_name or '复核' in file_name or '审计' in file_name:
            FileType = 3
        else:
            FileType = 2

        sql = "select 1 from dbo.DisclosureOfInformation where FileName=N'{}'".format(file_name.replace(".pdf", ""))
        print(sql)
        b1.execute(sql)
        res = b1.fetchone()
        if not res:
            sql = "insert into dbo.DisclosureOfInformation(TrustId,TrustCode,FileName,DisclosureTime,FileType) values({},'{}',N'{}','{}',{})".format(
                trust_id, trust_code, file_name.replace(".pdf", ""), web_date, FileType)
            print(sql)
            b1.execute(sql)
            conn.commit()
            print(trust_id, file_name, '披露信息插入完成!')
        else:
            print("DisclosureOfInformation已存在", file_name)

    # 更新状态信息
    b2 = conn.cursor()
    #file_name = file_name + '.pdf'
    sql = "select * from PortfolioManagement.DV.TrustAssociatedDocument where TrustId={} and FileName=N'{}'".format(
        trust_id, file_name)
    print(sql)
    b2.execute(sql)
    TrustDocumentID = b2.fetchone()[0]
    conn.commit()

    sql = "select 1 from TaskCollection.dbo.ProductsStateInformation where TrustDocumentID={}".format(TrustDocumentID)
    print(sql)
    b1.execute(sql)
    res = b1.fetchone()
    if not res:
        sql = "insert into TaskCollection.dbo.ProductsStateInformation values({},{},2,9)".format(
            trust_id,
            TrustDocumentID)
        print(sql)
        b2.execute(sql)
        conn.commit()
        print(trust_id, '状态信息插入完成')
    else:
        print("ProductsStateInformation已存在TrustDocumentID", TrustDocumentID)


# 上传FTP
def upload_file(remotepath, localpath, ftp_path2, trust_code, sqlfilepath, filename, ftp_file):
    ftp = FTP()
    host = '192.168.1.211'
    port = 21
    username = 'gsuser'
    password = 'Password01'
    ftp.connect(host, 21)
    ftp.login(username, password)
    try:
        ftp.mkd(ftp_path2)
        print('产品文件新建完成')
        ftp.cwd(ftp_path2)
        bufsize = 1024
        ftp.encoding = 'GBK'
        fp = open(localpath, 'rb')
        ftp.storbinary('STOR ' + remotepath, fp, bufsize)
        print('文件上传FTP成功')
        ftp.set_debuglevel(0)
        fp.close()
    except:
        print('产品文件新建失败')
        try:
            ftp.mkd(ftp_path2)
            print('FTP文件夹存在，已切换')
            bufsize = 1024
            ftp.encoding = 'GBK'
            fp = open(localpath, 'rb')
            ftp.storbinary('STOR ' + remotepath, fp, bufsize)
            print('文件上传FTP成功')
            ftp.set_debuglevel(0)
            fp.close()
            insert(trust_code, sqlfilepath, filename)
        except:
            print('类型文件存在')
            try:
                ftp.cwd(ftp_path2)
                print('FTP文件夹存在，已切换')
                bufsize = 1024
                ftp.encoding = 'GBK'
                fp = open(localpath, 'rb')
                ftp.storbinary('STOR ' + remotepath, fp, bufsize)
                print('文件上传FTP成功')
                ftp.set_debuglevel(0)
                fp.close()
                insert(trust_code, sqlfilepath, filename)
            except:
                print('上传产品文件失败')
                pass

# TrustAssociatedDocument表插入记录(DV页面显示)
def insert_db_record(trust_code, sqlfilepath, filename):
    b1 = conn.cursor()
    try:
        sql = "select Trustid from DV.view_Products where TrustCode='{}'".format(
            trust_code
        )
        print(sql)
        b1.execute(sql)
        trust_id = b1.fetchone()[0]

        sql = f"select count(1) from DV.TrustAssociatedDocument where Trustid={trust_id} and FileName=N'{filename}'"
        print(sql)
        b1.execute(sql)

        if b1.fetchone()[0] == 0:
            sql = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values({},'TrusteeReport','NULL','{}',N'{}','pdf',GETDATE(),'py')".format(
                trust_id, sqlfilepath, filename
            )
            print(sql)
            b1.execute(sql)
            conn.commit()
            print("记录插入成功")
        return True
    except:
        print(filename, "记录插入失败!")
        return False

# TrustAssociatedDocument表插入记录(DV页面显示)
def insert(trust_code, sqlfilepath, filename, web_date):
    conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
                           database='PortfolioManagement', charset='utf8')
    b1 = conn.cursor()
    try:
        sql = "select 1 from DV.TrustAssociatedDocument where FileName=N'{}'".format(filename)
        b1.execute(sql)
        res = b1.fetchone()
        if res:
            print('该文件名已有文档表记录', filename)
            return

        select_Trustid = "select Trustid from DV.view_Products where TrustCode='{}'".format(trust_code)
        b1.execute(select_Trustid)
        Trust_id = b1.fetchone()[0]
        b2 = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values({},'TrusteeReport','NULL','{}',N'{}','pdf',GETDATE(),'py')".format(
            Trust_id, sqlfilepath, filename)
        print(b2)
        b1.execute(b2)
        conn.commit()
        print('记录插入成功')



    except:
        print(filename, '记录插入失败!')


if __name__ == '__main__':
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
    use_selenium(proxies)

    ftp.close()
    ftp2.close()

    driver.close()
    driver.quit()
