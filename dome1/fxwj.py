import smtplib
from email.mime.text import MIMEText
from dateutil.parser import parse
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import requests
import os
import sys
from datetime import datetime
import logging
import pypinyin
import pymssql
from ftplib import FTP


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("发行文件日志记录.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
now_time = str(datetime.now())


def use_selenium():
    driver.get('http://www.chinabond.com.cn/Channel/21000')
    time.sleep(3)
    driver.find_element_by_id('span_21420').click()
    time.sleep(3)
    driver.switch_to.frame('ffrIframe')
    time.sleep(3)
    driver.find_element_by_id('keyword_21420').send_keys('发行文件')
    time.sleep(3)
    driver.find_element_by_class_name('fx_s_btn').click()
    time.sleep(3)


def update_pdf():
    soup = BeautifulSoup(driver.page_source, 'lxml')
    update_times = soup.select('#list_fxwj > li > span:nth-of-type(1)')
    update_links = soup.select('span.unlock > a')
    first_update_times = soup.select('#list_fxwj > li > span:nth-of-type(1)')[0].text
    print("这次更新的日期为" + first_update_times + "，将会写到pdf_time.txt中")
    # \\172.16.7.168\Shared\AnalysisFramework\资产证券化数据\信贷资产证券化\银行间债券市场
    with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\pdf_time.txt', 'r') as f:
        last_date = f.read()
        print("上次更新的日期为" + last_date)
    with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\pdf_time.txt', 'w') as f:
        f.write(first_update_times)
    for update_time, update_link in zip(update_times, update_links):
        data = {
            'update_time': update_time,
            'update_link': 'http://www.chinabond.com.cn' + update_link.get('href')
        }
        web_date = data.get('update_time').text
        # if web_date > '2023/06/08 11:30':
        #     continue
        if (web_date > last_date):
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
            wb_data = requests.get(data.get('update_link'), headers=headers)
            soup = BeautifulSoup(wb_data.text, 'lxml')
            Product_name = soup.title.text.strip()
            print(Product_name + '正在下载')
            # pdf_links = soup.select('ul > li > span > a')
            # pdf_titles = soup.select('ul > li > span > a')
            pdf_links = soup.select('ul > li > div > span > a')
            pdf_titles = soup.select('ul > li > div > span > a')
            for pdf_title, pdf_link in zip(pdf_titles, pdf_links):
                data = {
                    'title': pdf_title.get('title'),
                    'link': pdf_link.get('href')
                }
                File_Path = r'\\172.16.7.114\DataTeam\Products_log\银行间债券市场更新数据' + '\\{}\\'.format(
                    Product_name.split('证券')[0] + '证券')
                if not os.path.exists(File_Path):
                    os.makedirs(File_Path)

                rep = requests.get(data['link'], headers=headers)
                time.sleep(3)
                with open(File_Path + r'\{}'.format(data['title']), 'wb') as f:
                    f.write(rep.content)
                f.close()

                # 写入增量文件
                Zl_Path = r'\\172.16.7.114\DataTeam\增量文档'
                month = datetime.now().month
                Month_Path = os.path.join(Zl_Path, str(month))
                Month_File_Path = os.path.join(Month_Path, Product_name.split('证券')[0] + '证券')
                if not os.path.exists(Month_File_Path):
                    os.makedirs(Month_File_Path)
                rep = requests.get(data['link'], verify=False)
                time.sleep(2)
                with open(Month_File_Path + r'\{}'.format(data['title']), 'wb') as f:
                    f.write(rep.content)

            # 调用新建

            now_products(Product_name)
            if '更正' in Product_name or '更新' in Product_name:
                print(Product_name, '更正更新的产品不用新建')
                gzProd.append(Product_name)
            else:
                txt = TrustCode + '.txt'
                TrustCode_path = os.path.join(File_Path, txt)
                with open(TrustCode_path, 'w') as f:
                    f.write('自动创建')
                f.close()
                print('产品新建完成')

            for i in os.listdir(Month_File_Path):
                if '.pdf' in i:
                    if '评级报告' in i:
                        remotepath = './' + i
                        localpath = os.path.join(File_Path, i)
                        ftp_path2 = 'DealViewer/TrustAssociatedDoc/{}/ProductCreditRatingFiles/'.format(TrustCode)
                        ftp_file = 'DealViewer/TrustAssociatedDoc/{}/'.format(TrustCode)

                        sqlfilepath = 'DealViewer/TrustAssociatedDoc/{}/ProductCreditRatingFiles/'.format(TrustCode)

                        filename = i
                        # 文件上传
                        file_type = 'ProductCreditRatingFiles'
                        upload_file(remotepath, localpath, ftp_path2, TrustCode, sqlfilepath, filename, ftp_file,
                                    file_type, web_date)
                    else:
                        remotepath = './' + i
                        localpath = os.path.join(File_Path, i)
                        ftp_path2 = 'DealViewer/TrustAssociatedDoc/{}/ProductReleaseInstructions/'.format(TrustCode)
                        ftp_file = 'DealViewer/TrustAssociatedDoc/{}/'.format(TrustCode)

                        sqlfilepath = 'DealViewer/TrustAssociatedDoc/{}/ProductReleaseInstructions/'.format(TrustCode)

                        filename = i
                        # 文件上传
                        file_type = 'ProductReleaseInstructions'
                        upload_file(remotepath, localpath, ftp_path2, TrustCode, sqlfilepath, filename, ftp_file,
                                    file_type, web_date)

            with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\fx_FileName.txt', 'a') as f:
                f.write(now_time)
                f.write(Product_name)
                f.write('\n')
                f.close()
            with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\发行文件.xlsx', 'a') as f:
                f.write(now_time)
                f.write(Product_name)
                f.write('\n')
                f.close()

            # 发行产品写入
            with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\中国债券网产品.xlsx', 'a') as f:
                f.write(Product_name)
                f.write('\n')
                f.close()
                logger.info("Finish")
            try:
                if '更正' in Product_name or '更新' in Product_name:
                    print(Product_name, '更正更新的产品不插入数据库的披露表跟状态表')
                else:
                    InsertInformationInsert(TrustCode, web_date)
            except:
                print(TrustCode, '披露信息插入失败!')

        else:
            print('无更新')
            logger.info("Start print log")
            logger.info("Finish")


# 转换中文数值为阿拉伯数值
def conversion(str):
    if type(str) == int:
        return str
    else:

        zhong = {'零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9};
        danwei = {'十': 10, '百': 100, '千': 1000, '万': 10000};
        num = 0;
        if len(str) == 0:
            return 0;
        if len(str) == 1:
            if str == '十':
                return 10;
            num = zhong[str];
            return num;
        temp = 0;
        if str[0] == '十':
            num = 10;
        for i in str:
            if i == '零':
                temp = zhong[i];
            elif i == '一':
                temp = zhong[i];
            elif i == '二':
                temp = zhong[i];
            elif i == '三':
                temp = zhong[i];
            elif i == '四':
                temp = zhong[i];
            elif i == '五':
                temp = zhong[i];
            elif i == '六':
                temp = zhong[i];
            elif i == '七':
                temp = zhong[i];
            elif i == '八':
                temp = zhong[i];
            elif i == '九':
                temp = zhong[i];
            if i == '十':
                temp = temp * danwei[i];
                num += temp;
            elif i == '百':
                temp = temp * danwei[i];
                num += temp;
            elif i == '千':
                temp = temp * danwei[i];
                num += temp;
            elif i == '万':
                temp = temp * danwei[i];
                num += temp;
        if str[len(str) - 1] != '十' and str[len(str) - 1] != '百' and str[len(str) - 1] != '千' and str[
            len(str) - 1] != '万':
            num += temp;
        return num;


# Trust表插入新产品数据
def now_products(Product_name):
    global TrustCode
    if '消费贷款' in Product_name:
        FCode = 'ConsumerLoan'
    elif '信用卡' in Product_name:
        FCode = 'ConsumerLoan'
    elif '信贷资产' in Product_name:
        FCode = 'CreditLoan'
    elif '住房抵押贷款' in Product_name:
        FCode = 'RMBS'
    elif '汽车抵押贷款' in Product_name:
        FCode = 'AUTO'
    elif '汽车贷款' in Product_name:
        FCode = 'AUTO'
    elif '汽车分期贷款' in Product_name:
        FCode = 'AUTO'
    elif '微小企业贷款' in Product_name:
        FCode = 'SmallLoan'
    elif '租赁资产' in Product_name:
        FCode = 'Lease'
    elif '不良资产' in Product_name:
        FCode = 'NPL'
    else:
        FCode = ''

    try:
        s = ''
        for i in pypinyin.pinyin(Product_name, style=pypinyin.NORMAL):
            i = i[0].title()
            s += ''.join(i)

        s = s.split('Nian')[0]

        sp_filename = Product_name.split('第')[1]
        nper = sp_filename.split('期')[0]
        conversion_nper = conversion(nper)
        STrustCode = s + '-' + str(conversion_nper)

        TrustCode1 = STrustCode.split('2023')[0]
        TrustCode2 = STrustCode.split('2023')[-1]

        TrustCode = TrustCode1 + '_' + FCode + '2023' + TrustCode2
        print(TrustCode)

        if '更正' in Product_name or '更新' in Product_name:
            return
        splitname = Product_name.split('年')[0]
        TrustNameShort = splitname + '-' + str(conversion_nper)
        TrustName = Product_name.split('发行文件')[0]
    except:
        print('TrustCode\TrustName获取失败')
        return

    # if Product_name=='华驭第十四期汽车抵押贷款支持证券发行文件':
    #     TrustName='华驭第十四期汽车抵押贷款支持证券'
    #     TrustNameShort='华驭-14'
    #     TrustCode='HuaYu_AUTO-14'


    def AType():
        if '住房' in TrustName:
            AssetType = 'HouseLoan'
        elif ('个人消费' or '信用卡分期') in TrustName:
            AssetType = 'ConsumerLoan'
        elif '汽车' in TrustName:
            AssetType = 'CarLoan'
        elif '信贷' in TrustName:
            AssetType = 'CreditLoan'
        elif '微小企业' in TrustName:
            AssetType = 'SmallLoan'
        elif '租赁资产' in TrustName:
            AssetType = 'Lease'
        elif '不良资产' in TrustName:
            AssetType = 'NPL'
        else:
            AssetType = 'null'
        return AssetType

    conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
                           database='PortfolioManagement', charset='utf8')
    b2 = conn.cursor()

    sql_check = "select 1 from TrustManagement.Trust where Trustcode='{}'".format(TrustCode)
    b2.execute(sql_check)
    if b2.fetchone():
        print('code重名', TrustCode)
        cmProd.append(Product_name + ':' + TrustCode)
        return

    sql1 = " select max(TrustId)+1 from TrustManagement.Trust where TrustId<50000"
    b2.execute(sql1)
    TrustId = b2.fetchone()[0]

    sql = '''
        SET IDENTITY_INSERT TrustManagement.Trust ON ;
        insert into TrustManagement.Trust(TrustId,TrustCode,TrustName,TrustNameShort,IsMarketProduct,TrustStatus) values({},'{}',N'{}',N'{}',1,'Duration');
        SET IDENTITY_INSERT TrustManagement.Trust OFF ;
    '''.format(TrustId, TrustCode, TrustName, TrustNameShort)

    b2.execute(sql)

    conn.commit()

    sql2 = '''
        SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust ON ;
        insert into FixedIncomeSuite.Analysis.Trust(TrustId,TrustCode,TrustName)
        select TrustId,TrustCode,TrustName from [DV].[view_Products] where TrustId={};
        SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust OFF ;
    '''.format(TrustId)
    b2.execute(sql2)
    conn.commit()
    sql2 = "insert into TrustManagement.TrustInfoExtension(TrustId, StartDate, EndDate, ItemId, ItemCode, ItemValue) values ({}, GETDATE(), null, null, 'MarketCategory','CAS'),({}, GETDATE(), null, null, 'RegulatoryOrg','CBIRC'),({}, GETDATE(), null, null, 'MarketPlace', 'InterBank'),({}, GETDATE(), null, null, 'AssetType','{}'),({}, GETDATE(), null, null, 'BasicAssetType','Others'),({}, GETDATE(), null, null, 'CollectionMethod', 'PublicOffering')".format(
        TrustId, TrustId, TrustId, TrustId, AType(), TrustId, TrustId)
    b2.execute(sql2)
    conn.commit()
    conn.close()

    # 调用函数插入最大期数表
    instrtMaxNper(TrustId)


# 上传FTP
def upload_file(remotepath, localpath, ftp_path2, TrustCode, sqlfilepath, filename, ftp_file, file_type, web_date):
    ftp = FTP()
    host = '192.168.1.211'
    port = 21
    username = 'gsuser'
    password = 'Password01'
    ftp.connect(host, 21)
    ftp.login(username, password)
    try:
        insert(TrustCode, sqlfilepath, filename, file_type, web_date)

        print('产品文件新建完成')
        try:
            ftp.mkd(ftp_file)
        except:
            print('211文件根目录存在')
        try:
            ftp.mkd(ftp_path2)
        except:
            print('211文件子目录存在')
        ftp.cwd(ftp_path2)
        bufsize = 1024
        ftp.encoding = 'GBK'
        fp = open(localpath, 'rb')
        ftp.storbinary('STOR ' + remotepath, fp, bufsize)
        print('文件上传FTP成功')
        ftp.set_debuglevel(0)
        fp.close()

    except Exception as e:
        print('产品上任务系统失败')
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


# TrustAssociatedDocument表插入记录（DV页面显示）
def insert(TrustCode, sqlfilepath, filename, file_type, web_date):
    conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
                           database='PortfolioManagement', charset='utf8')
    b1 = conn.cursor()

    select_Trustid = "select Trustid from DV.view_Products where TrustCode='{}'".format(TrustCode)
    b1.execute(select_Trustid)
    Trust_id = b1.fetchone()[0]
    print(Trust_id, file_type, sqlfilepath, filename)
    b2 = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values({},'{}','NULL','{}',N'{}','pdf',GETDATE(),'py')".format(
        Trust_id, file_type, sqlfilepath, filename)
    # print(sqlfilepath)
    # print(filename)
    print(b2)
    b1.execute(b2)
    print('记录插入成功')
    print('\n')
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
    conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
                           database='PortfolioManagement', charset='utf8')
    b2 = conn.cursor()

    sql1 = "select TrustNameShort from TrustManagement.Trust where TrustId={}".format(TrustId)
    b2.execute(sql1)
    TrustNameShort = b2.fetchone()[0]

    sql = "insert into dbo.ReportMaxNper values({},N'{}',{},N'存续期')".format(TrustId, TrustNameShort, 0)
    b2.execute(sql)
    conn.commit()


# 披露信息插入(任务分配系统显示)
def InsertInformationInsert(TrustCode, web_date):
    conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
                           database='PortfolioManagement', charset='utf8')
    b1 = conn.cursor()

    selectName = "select TrustName,TrustId from TrustManagement.Trust where TrustCode='{}'".format(TrustCode)
    b1.execute(selectName)
    TrustName, TrustId = b1.fetchone()

    s_TrustDocumentID = "select TrustDocumentID,FileName from PortfolioManagement.DV.TrustAssociatedDocument where TrustId={} and FileName like N'%说明书%'".format(
        TrustId)
    b1.execute(s_TrustDocumentID)
    TrustDocumentID, FileName = b1.fetchone()
    FileName = FileName[:-4]
    Insert_ProductStatus = "insert into TaskCollection.dbo.ProductsStateInformation values({},{},1,9)".format(TrustId,
                                                                                                              TrustDocumentID)
    b1.execute(Insert_ProductStatus)
    conn.commit()
    web_date = parse(str(web_date))
    Insert = "insert into dbo.DisclosureOfInformation(TrustId,TrustCode,FileName,DisclosureTime,FileType) values({},'{}',N'{}','{}',1)".format(
        TrustId, TrustCode, FileName, web_date)
    b1.execute(Insert)
    conn.commit()
    print(TrustId, TrustCode, TrustName, '产品信息插入完成!')
    print('\n')

    print(TrustId, '状态信息插入完成')


def mail(gzProd, cmProd):
    msg_from = 'yixianfeng@goldenstand.cn'  # 发送方邮箱
    passwd = 'icRKJbiDUPCERBS8'  # 填入发送方邮箱的授权码
    msg_to1 = ['wangxin@goldenstand.cn', 'fengyanyan@goldenstand.cn', 'yangshu@goldenstand.cn']

    content = '{}\n'.format(time.strftime("%Y-%m-%d")) + '债券网更新更正以及code重名产品情况:' + '\n'

    content += '更新更正产品:' + str(len(gzProd)) + '个' + '\n'
    for i in gzProd:
        content += i
        content += '\n'

    content += 'code重名产品:' + str(len(cmProd)) + '个' + '\n'
    for i in cmProd:
        content += i
        content += '\n'

    for msg_to in msg_to1:
        subject = "债券网更新更正以及code重名产品情况"  # 主题
        msg = MIMEText(content)
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


if __name__ == '__main__':
    options = webdriver.ChromeOptions()
    options.add_argument(
        'user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"')
    driver = webdriver.Chrome(executable_path=r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe')
    if os.path.exists(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\发行文件.xlsx') == True:
        os.remove(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\发行文件.xlsx')
    gzProd, cmProd = [], []
    use_selenium()
    update_pdf()

    err_fxwj=[gzProd,cmProd]
    import pickle

    with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\err_fxwj.pkl', 'wb') as f:
        pickle.dump(err_fxwj, f)
    # if gzProd or cmProd:
    #     mail(gzProd, cmProd)

    time.sleep(3)
    driver.close()
    driver.quit()
