import smtplib
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import requests
import os
import sys
from ftplib import FTP
import pymssql
from datetime import datetime, date, timedelta
from dateutil.parser import parse
from email.mime.text import MIMEText

now_time = str(datetime.now())


def use_selenium():
    driver.get('http://www.chinabond.com.cn/Channel/21000')
    time.sleep(3)
    driver.find_element_by_id('span_21420').click()
    time.sleep(3)
    driver.switch_to.frame('ffrIframe')
    time.sleep(1)
    driver.find_element_by_xpath('//*[@id="fxdf"]/a/span').click()
    time.sleep(3)
    # 定位输入框
    driver.find_element_by_id('keyword_21420').send_keys()
    # 勾选
    time.sleep(3)
    driver.find_element_by_class_name('fx_s_btn').click()
    time.sleep(3)
    # range(1,40)除去i==3(翻页出现的"上一页"导致的偏移)读取了38页的数据,应该够了
    for i in range(1, 40):
        if i == 3:
            continue
        print(i)
        driver.find_element_by_xpath('//*[@id="pg_fxwj"]/li[{}]'.format(i)).click()
        update_pdf()


def date():
    driver.get('http://www.chinabond.com.cn/Channel/21000')
    time.sleep(3)
    driver.find_element_by_id('span_21420').click()
    time.sleep(3)
    driver.switch_to.frame('ffrIframe')
    time.sleep(1)
    driver.find_element_by_xpath('//*[@id="fxdf"]/a/span').click()
    time.sleep(3)
    # 定位输入框,输入发行文件
    soup = BeautifulSoup(driver.page_source, 'lxml')
    driver.find_element_by_id('keyword_21420')
    first_update_times = soup.select('#list_fxwj > li > span:nth-of-type(1)')[0].text
    with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\受托报告更新时间.txt', 'w') as f:
        f.write(first_update_times)
    f.close()


def update_pdf():
    with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\受托报告更新时间.txt', 'r') as f:
        last_date = f.read()
        print("上次更新的日期为" + last_date)
    f.close()
    soup = BeautifulSoup(driver.page_source, 'lxml')
    update_times = soup.select('#list_fxwj > li > span:nth-of-type(1)')
    update_links = soup.select('span.unlock > a')
    update_titles = soup.select('span.unlock > a')

    for update_time, update_link, update_title in zip(update_times, update_links, update_titles):

        data = {
            'update_time': update_time,
            'update_link': 'http://www.chinabond.com.cn' + update_link.get('href'),
            'update_title': update_title.get('title')
        }
        web_date = data.get('update_time').text
        FileName1 = data['update_title']

        # if web_date >= '2022/12/19 15:06':
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
                            time.sleep(2)
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
                        with open('D:\Work\DataTeam\Products_log\更新时间TXT记录\FileName.txt', 'a') as f:
                            f.write(now_time)
                            f.write(data['update_title'])
                            f.write('\n')
                            f.close()

                        with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\更新受托报告.xlsx', 'a') as f:
                            f.write(now_time)
                            f.write(data['update_title'])
                            f.write('\n')
                            f.close()

                        ftp_path = 'DealViewer\TrustAssociatedDoc'
                        folder = 'TrusteeReport'
                        for file in os.listdir(pathname):
                            if '.txt' in file:
                                TrustCode = file.split('.')[0]
                                # print(TrustCode)
                                ftp_path1 = os.path.join(ftp_path, TrustCode)
                                # print(ftp_path1)
                                ftp_path2 = os.path.join(ftp_path1, folder)
                                # print(ftp_path2)
                                sqlfilepath = ftp_path2 + '/'
                                sqlfilepath = sqlfilepath.replace('\\', '/')

                                remotepath = './' + filename
                                localpath = os.path.join(pathname, filename)
                                ftp_file = 'DealViewer\TrustAssociatedDoc{}'.format(TrustCode)
                                upload_file(remotepath, localpath, ftp_path2, TrustCode, sqlfilepath, filename,
                                            ftp_file)
                                # print(remotepath,localpath,ftp_path2,TrustCode,sqlfilepath,filename,ftp_file)

                                insert(TrustCode, sqlfilepath, filename, web_date)
                                try:
                                    updataMaxNper(TrustCode)
                                except:
                                    print(TrustCode, '期数更新失败')
                                    pass
                                try:
                                    InformationInsert(TrustCode, FileName1, web_date)
                                except:
                                    print(TrustCode, '披露信息插入失败!')
                                    pass
                        break
            if found == 0:
                notFound.append(data['update_title'])

        else:
            date()
            err_stbg=[mailFiles,notFound]
            import pickle

            with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\err_stbg.pkl', 'wb') as f:
                pickle.dump(err_stbg, f)
            sys.exit(0)


# 更新最大期数
def updataMaxNper(TrustCode):
    conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
                           database='PortfolioManagement', charset='utf8')
    b2 = conn.cursor()

    sql = "select TrustId from TrustManagement.Trust where TrustCode='{}'".format(TrustCode)
    b2.execute(sql)
    TrustId = b2.fetchone()[0]
    print(TrustId)

    TrustNameShortSQL = "select TrustNameShort from TrustManagement.Trust where TrustId={}".format(TrustId)
    b2.execute(TrustNameShortSQL)
    TrustNameShort = b2.fetchone()[0]
    print(TrustNameShort)

    # 判断最大期数表中是否存在该产品
    selectMaxNper = "select TrustId from dbo.ReportMaxNper"
    b2.execute(selectMaxNper)
    SMaxNperId = b2.fetchall()
    conn.commit()
    MaxNperId = []
    for NperId in SMaxNperId:
        MaxNperId.append(NperId[0])
    if TrustId in MaxNperId:
        updatenper = "update dbo.ReportMaxNper set MaxNper += 1 where TrustId ={}".format(TrustId)
        b2.execute(updatenper)
        print('期数更新完成!')
    else:
        Insert = "insert into dbo.ReportMaxNper values({},N'{}',1,N'{}')".format(TrustId, TrustNameShort, '存续期')
        b2.execute(Insert)
        conn.commit()

    SelectNper = "select MaxNper from dbo.ReportMaxNper where TrustId={}".format(TrustId)
    b2.execute(SelectNper)
    MaxNper = b2.fetchone()[0]
    conn.commit()
    conn.close()
    print('最大期数为{}更新成功！'.format(MaxNper))
    print('\n')


# 爬取披露信息插入（任务分配系统显示） ++状态信息
def InformationInsert(TrustCode, FileName1, web_date):
    conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
                           database='PortfolioManagement', charset='utf8')
    b1 = conn.cursor()
    selectId = "select TrustId from TrustManagement.Trust where TrustCode='{}'".format(TrustCode)
    b1.execute(selectId)
    TrustId = b1.fetchone()[0]
    if TrustId in(10804,10759):
        print('10804,10759不上任务系统')
        return
    web_date = str(web_date)

    web_date = parse(str(web_date))
    selectDisclosureTime = "select max(DisclosureTime) from dbo.DisclosureOfInformation where TrustId={}".format(
        TrustId)
    b1.execute(selectDisclosureTime)
    SDisclosureTimeMax = b1.fetchone()[0]

    web_date = parse(str(web_date))
    SDisclosureTimeMax = parse(str(SDisclosureTimeMax))
    cz = (web_date - SDisclosureTimeMax).days
    if cz < 17 or '更正' in mailFiles:
        mailFiles.append(FileName1)
        print('文件披露可能重复!')
    elif '半年' in FileName1:
        print('半年受托报告不需要披露')
    else:
        if '清算' in FileName1 or '复核' in FileName1 or '审计' in FileName1:
            FileType = 3
        else:
            FileType = 2

        sql = "select 1 from dbo.DisclosureOfInformation where FileName=N'{}'".format(FileName1)
        b1.execute(sql)
        res = b1.fetchone()
        if not res:
            Insert = "insert into dbo.DisclosureOfInformation(TrustId,TrustCode,FileName,DisclosureTime,FileType) values({},'{}',N'{}','{}',{})".format(
                TrustId, TrustCode, FileName1, web_date, FileType)
            b1.execute(Insert)
            conn.commit()
            print(TrustId, FileName1, '披露信息插入完成!')
            print('\n')

    # 更新状态信息
    conn1 = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
                            database='TaskCollection', charset='utf8')
    b2 = conn1.cursor()
    FileName1 = FileName1 + '.pdf'
    TrustDocumentID = "select * from PortfolioManagement.DV.TrustAssociatedDocument where TrustId={} and FileName=N'{}'".format(
        TrustId, FileName1)
    b2.execute(TrustDocumentID)
    TrustDocumentID = b2.fetchone()[0]
    conn.commit()

    sql = "select 1 from TaskCollection.dbo.ProductsStateInformation where TrustDocumentID={}".format(TrustDocumentID)
    b1.execute(sql)
    res = b1.fetchone()
    if not res:
        Insert_ProductStatus = "insert into TaskCollection.dbo.ProductsStateInformation values({},{},2,9)".format(
            TrustId,
            TrustDocumentID)
        b2.execute(Insert_ProductStatus)
        conn1.commit()
        print(TrustId, '状态信息插入完成')


# 上传FTP
def upload_file(remotepath, localpath, ftp_path2, TrustCode, sqlfilepath, filename, ftp_file):
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
            insert(TrustCode, sqlfilepath, filename)
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
                insert(TrustCode, sqlfilepath, filename)
            except:
                print('上传产品文件失败')
                pass


# TrustAssociatedDocument表插入记录(DV页面显示)
def insert(TrustCode, sqlfilepath, filename, web_date):
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

        select_Trustid = "select Trustid from DV.view_Products where TrustCode='{}'".format(TrustCode)
        b1.execute(select_Trustid)
        Trust_id = b1.fetchone()[0]
        b2 = "insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values({},'TrusteeReport','NULL','{}',N'{}','pdf',GETDATE(),'py')".format(
            Trust_id, sqlfilepath, filename)
        b1.execute(b2)
        conn.commit()
        print('记录插入成功')



    except:
        print(filename, '记录插入失败!')


if __name__ == '__main__':
    options = webdriver.ChromeOptions()
    options.add_argument(
        'user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"')
    driver = webdriver.Chrome(executable_path=r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe')
    if os.path.exists(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\更新受托报告.xlsx') == True:
        os.remove(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\更新受托报告.xlsx')
    mailFiles = []
    notFound = []
    use_selenium()
    driver.close()
    driver.quit()
