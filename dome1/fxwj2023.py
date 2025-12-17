import smtplib
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


def list_ftp_directory_with_retry(ftp, path, retries=3):
    for attempt in range(retries):
        try:
            return list_ftp_directory(ftp, path)
        except (ftplib.error_temp, ftplib.error_perm, BrokenPipeError) as e:
            print(
                f"Temporary error occurred: {e}. Retrying {retries - attempt - 1} more times."
            )
            time.sleep(5)  # Wait for 5 seconds before retrying

    raise Exception("Failed to list directory after multiple attempts")


def list_ftp_directory(ftp, path):
    """List files and directories in the given FTP path."""
    print("listing directories in", path)

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
        print("Decoding with detected encoding failed, trying utf-8")
        items = raw_data.decode("utf-8", errors="ignore").split("\r\n")

    return items


def read_ftp_file(ftp, file_path):
    with io.BytesIO() as bio:
        ftp.retrbinary(f"RETR {file_path}", bio.write)
        bio.seek(0)
        return bio.read().decode("utf-8")


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
    latest_date_time = last_date  # Initialize with the last date in case no new data is found
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
    # get the pdf file content from web (using proxy)
    # curl -x http://splci64blr:6j0z1hrwFM4LbdheZ_@gate.smartproxy.com:10001 -I https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/zjzczq_ABS/ABS_fxwj_ath/202405/P020240527386727954575.pdf -v
    print("requests.get")
    response = requests.get(
        encoded_url, headers=headers, proxies=proxies, timeout=(10, 30)
    )

    # Check if the content is actually a PDF
    print("check content")
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

        #     with open(r'D:\DataTeam\Products_log\更新时间TXT记录\fx_FileName.txt', 'a') as f:
        #         f.write(now_time)
        #         f.write(product_name)
        #         f.write('\n')
        #         f.close()
        #     with open(r'D:\DataTeam\Products_log\更新时间TXT记录\发行文件.xlsx', 'a') as f:
        #         f.write(now_time)
        #         f.write(product_name)
        #         f.write('\n')
        #         f.close()

        #     # 发行产品写入
        #     with open(r'D:\DataTeam\Products_log\更新时间TXT记录\中国债券网产品.xlsx', 'a') as f:
        #         f.write(product_name)
        #         f.write('\n')
        #         f.close()
        #         logger.info("Finish")
        #     try:
        #         if '更正' in product_name or '更新' in product_name:
        #             print(product_name, '更正更新的产品不插入数据库的披露表跟状态表')
        #         else:
        #             InsertInformationInsert(trust_code, web_date)
        #     except:
        #         print(trust_code, '披露信息插入失败!')

        # else:
        #     record_latest_time()
        #     logger.info("Start print log")
        #     logger.info("Finish")
        #     driver.close()
        #     sys.exit(0)


def upload_file_to_ftp(ftp, local_file_path, ftp_folder, ftp_file_path, file_name):
    # if file does not exist on FTP, upload the file
    if file_name not in list_ftp_directory_with_retry(ftp, ftp_folder):
        print("Writing PDF to", ftp_file_path, "=====>")
        with open(local_file_path, "rb") as f:
            ftp.storbinary(f"STOR {ftp_file_path}", f)
    else:
        print("File already exists on FTP in folder:", ftp_folder)


def update_pdf():
    file_path = ""
    month_file_path = ""
    soup = BeautifulSoup(driver.page_source, "lxml")
    update_times = soup.select("#list_list > li > span:nth-of-type(1)")
    update_links = soup.select("span.unlock > a")
    first_update_times = soup.select("#list_list > li > span:nth-of-type(1)")[0].text
    print("这次更新的日期为" + first_update_times + "，将会写到pdf_time.txt中")

    # \\172.16.7.168\Shared\AnalysisFramework\资产证券化数据\信贷资产证券化\银行间债券市场
    with open(r"D:\DataTeam\Products_log\更新时间TXT记录\pdf_time.txt", "r") as f:
        last_date = f.read()
        print("上次更新的日期为" + last_date)

    for update_time, update_link in zip(update_times, update_links):
        data = {"update_time": update_time, "update_link": update_link.get("href")}
        web_date = data.get("update_time").text

        if web_date > last_date:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"
            }
            wb_data = requests.get(data.get("update_link"), headers=headers)
            wb_data.encoding = "utf-8"
            soup = BeautifulSoup(wb_data.text, "lxml")

            url_prefix = data.get("update_link").replace(
                data.get("update_link").split("/")[-1], ""
            )
            product_name = soup.select("span.biaoti")[0].text.strip()
            print(product_name + "正在下载")
            span_contents = soup.find_all("span", class_="rightFileImport")
            # print(span_contents)
            for content in span_contents:
                a = content.find("a")
                data = {
                    "title": a.text.strip(),
                    "link": urljoin(url_prefix, a.get("href")),
                }
                print(data["title"], data["link"])
                file_path = (
                    r"\\172.16.7.114\DataTeam\Products_log\银行间债券市场更新数据"
                    + "\\{}\\".format(product_name.split("证券")[0] + "证券")
                )
                if not os.path.exists(file_path):
                    os.makedirs(file_path)

                rep = requests.get(data["link"], headers=headers)
                time.sleep(4)
                with open(file_path + r"\{}".format(data["title"]), "wb") as f:
                    f.write(rep.content)
                f.close()

                # 写入增量文件
                zl_path = r"\\172.16.7.114\DataTeam\增量文档"
                month = datetime.now().month
                month_path = os.path.join(zl_path, str(month))
                month_file_path = os.path.join(
                    month_path, product_name.split("证券")[0] + "证券"
                )
                print(month_file_path)
                if not os.path.exists(month_file_path):
                    os.makedirs(month_file_path)
                rep = requests.get(data["link"])
                time.sleep(3)
                with open(month_file_path + r"\{}".format(data["title"]), "wb") as f:
                    f.write(rep.content)

            # 调用新建

            create_new_product(product_name)
            if "更正" in product_name or "更新" in product_name:
                print(product_name, "更正更新的产品不用新建")
                gzProd.append(product_name)
            else:
                txt = trust_code + ".txt"
                trust_code_path = os.path.join(file_path, txt)
                with open(trust_code_path, "w") as f:
                    f.write("自动创建")
                f.close()
                print("产品新建完成")

            for i in os.listdir(month_file_path):
                if ".pdf" in i:
                    if "评级报告" in i:
                        remotepath = "./" + i
                        local_path = os.path.join(file_path, i)
                        ftp_path2 = "DealViewer/TrustAssociatedDoc/{}/ProductCreditRatingFiles/".format(
                            trust_code
                        )
                        ftp_file = "DealViewer/TrustAssociatedDoc/{}/".format(
                            trust_code
                        )

                        sql_file_path = "DealViewer/TrustAssociatedDoc/{}/ProductCreditRatingFiles/".format(
                            trust_code
                        )

                        file_name = i
                        # 文件上传
                        file_type = "ProductCreditRatingFiles"
                        upload_file(
                            remotepath,
                            local_path,
                            ftp_path2,
                            trust_code,
                            sql_file_path,
                            file_name,
                            ftp_file,
                            file_type,
                            web_date,
                        )
                    else:
                        remotepath = "./" + i
                        local_path = os.path.join(file_path, i)
                        ftp_path2 = "DealViewer/TrustAssociatedDoc/{}/ProductReleaseInstructions/".format(
                            trust_code
                        )
                        ftp_file = "DealViewer/TrustAssociatedDoc/{}/".format(
                            trust_code
                        )

                        sql_file_path = "DealViewer/TrustAssociatedDoc/{}/ProductReleaseInstructions/".format(
                            trust_code
                        )

                        file_name = i
                        # 文件上传
                        file_type = "ProductReleaseInstructions"
                        upload_file(
                            remotepath,
                            local_path,
                            ftp_path2,
                            trust_code,
                            sql_file_path,
                            file_name,
                            ftp_file,
                            file_type,
                            web_date,
                        )

            with open(
                r"D:\DataTeam\Products_log\更新时间TXT记录\fx_FileName.txt", "a"
            ) as f:
                f.write(now_time)
                f.write(product_name)
                f.write("\n")
                f.close()
            with open(
                r"D:\DataTeam\Products_log\更新时间TXT记录\发行文件.xlsx", "a"
            ) as f:
                f.write(now_time)
                f.write(product_name)
                f.write("\n")
                f.close()

            # 发行产品写入
            with open(
                r"D:\DataTeam\Products_log\更新时间TXT记录\中国债券网产品.xlsx", "a"
            ) as f:
                f.write(product_name)
                f.write("\n")
                f.close()
                logger.info("Finish")
            try:
                if "更正" in product_name or "更新" in product_name:
                    print(product_name, "更正更新的产品不插入数据库的披露表跟状态表")
                else:
                    insert_task_info(trust_code, web_date)
            except:
                print(trust_code, "披露信息插入失败!")

        else:
            record_latest_time()
            logger.info("Start print log")
            logger.info("Finish")
            driver.close()
            sys.exit(0)


def record_latest_time():
    # 选择下拉框回到第一页，获取最新的日期
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//select[@id='sel']/option[1]"))
    )
    element.click()
    time.sleep(6)

    soup = BeautifulSoup(driver.page_source, "lxml")
    first_update_times = soup.select("#list_list > li > span:nth-of-type(1)")[0].text
    with open(r"D:\DataTeam\Products_log\更新时间TXT记录\pdf_time.txt", "w") as f:
        f.write(first_update_times)
    f.close()


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

    sql_check = "select 1 from TrustManagement.Trust where Trustcode='{}'".format(
        trust_code
    )
    print(sql_check)
    b2.execute(sql_check)
    if b2.fetchone():
        print("code重名", trust_code)
        return trust_code

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


def upload_to_ftp(ftp, local_file_path, remote_dir, remote_file_path):
    bufsize = 1024

    try:
        try:
            ftp.mkd(remote_dir)
        except:
            print("FTP文件夹已存在")

        ftp.cwd(remote_dir)

        with open(local_file_path, "rb") as fp:
            ftp.storbinary("STOR " + remote_file_path, fp, bufsize)
            print("Uploaded to FTP: ", remote_file_path)

    except Exception as e:
        print("Failed to upload to FTP: ", remote_file_path)
        print(e)


# 上传FTP
def upload_file(
    remotepath,
    localpath,
    ftp_path2,
    trust_code,
    sqlfilepath,
    filename,
    ftp_file,
    file_type,
    web_date,
):
    ftp = FTP()
    host = "192.168.1.211"
    port = 21
    username = "gsuser"
    password = "Password01"
    ftp.connect(host, 21)
    ftp.login(username, password)
    try:
        insert(trust_code, sqlfilepath, filename, file_type, web_date)

        print("产品文件新建完成")
        try:
            ftp.mkd(ftp_file)
        except:
            print("211文件根目录存在")
        try:
            ftp.mkd(ftp_path2)
        except:
            print("211文件子目录存在")
        ftp.cwd(ftp_path2)
        bufsize = 1024
        ftp.encoding = "GBK"
        fp = open(localpath, "rb")
        ftp.storbinary("STOR " + remotepath, fp, bufsize)
        print("文件上传FTP成功")
        ftp.set_debuglevel(0)
        fp.close()

    except Exception as e:
        print("产品上任务系统失败")
        print(e)
        # try:
        #     ftp.mkd(ftp_path2)
        #     print('FTP文件夹存在，已切换')
        #     bufsize = 1024
        #     ftp.encoding = 'GBK'
        #     fp = open(localpath, 'rb')
        #     ftp.storbinary('STOR ' + remotepath, fp, bufsize)
        #     print('文件上传FTP成功')
        #     ftp.set_debuglevel(0)
        #     fp.close()
        #     insert(TrustCode,sqlfilepath, filename,file_type,web_date)
        # except:
        #     print('类型文件存在')
        #     try:
        #         ftp.cwd(ftp_path2)
        #         print('FTP文件夹存在，已切换')
        #         bufsize = 1024
        #         ftp.encoding = 'GBK'
        #         fp = open(localpath, 'rb')
        #         ftp.storbinary('STOR ' + remotepath, fp, bufsize)
        #         print('文件上传FTP成功')
        #         ftp.set_debuglevel(0)
        #         fp.close()
        #         insert(TrustCode, sqlfilepath, filename,file_type,web_date)
        #     except:
        #         print('上传产品文件失败')
        #         pass


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
