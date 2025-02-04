from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import requests
import os
import sys
from ftplib import FTP
import pandas as pd
import pymssql
from datetime import datetime, date, timedelta
from dateutil.parser import parse
now_time = str(datetime.now())

def use_selenium():
    # driver = webdriver.Chrome()
    driver.get('http://www.chinabond.com.cn/Channel/21000')
    time.sleep(3)
    driver.find_element(By.ID,'span_21420').click()
    time.sleep(3)
    driver.switch_to.frame('ffrIframe')
    time.sleep(1)
    driver.find_element_by_xpath('//*[@id="fxjg"]/a/span').click()
    time.sleep(3)
    # 定位输入框
    driver.find_element(By.ID,'keyword_21420').send_keys()
    # 勾选
    time.sleep(3)
    driver.find_element_by_class_name('fx_s_btn').click()
    time.sleep(3)
    update_pdf()
    # for i in [1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,32,33,34,35,36,37,38]:
    #     print(i)
    #     driver.find_element_by_xpath('//*[@id="pg_fxwj"]/li[{}]'.format(i)).click()
    #     update_pdf()


def update_pdf():
    # with open( r'D:\Work\DataTeam\Products_log\更新时间TXT记录\受托报告更新时间.txt', 'r') as f:
    #     last_date=f.read()
    #     print("上次更新的日期为"+last_date)
    # f.close()
    soup=BeautifulSoup(driver.page_source,'lxml')
    update_times=soup.select('#list_fxwj > li > span:nth-of-type(1)')
    update_links=soup.select('span.unlock > a')
    update_titles = soup.select('span.unlock > a')
    first_update_times = soup.select('#list_fxwj > li > span:nth-of-type(1)')[0].text
    # first_update_times = soup.select('#list_fxwj > li > span:nth-of-type(1)')[0].text.split(' ')[0]
    #print("这次更新的日期为"+first_update_times+"，将会写到pdf_time.txt中")
    # \\172.16.7.168\Shared\AnalysisFramework\资产证券化数据\信贷资产证券化\银行间债券市场

    # f.close()
    for update_time,update_link,update_title in zip(update_times,update_links,update_titles):

        data={
            'update_time':update_time,
            'update_link':'http://www.chinabond.com.cn'+update_link.get('href'),
            'update_title':update_title.get('title')
        }
        web_date = data.get('update_time').text
        FileName1=data['update_title']
        # last_date='2019/10/18 16:35'
        # web_date = data.get('update_time').text.split(' ')[0]
        # last_date='2018/01/19 15:23'
        with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\发行结果更新时间.txt', 'r') as f:
            last_date = f.read()
        if (web_date>last_date):
            # with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\发行结果更新时间.txt', 'w') as f:
            #     f.write(web_date)
            file_dirs = [
                r"\\172.16.7.114\DataTeam\Products\不良资产",
                r"\\172.16.7.114\DataTeam\Products\车贷",
                r"\\172.16.7.114\DataTeam\Products\房贷",
                r"\\172.16.7.114\DataTeam\Products\企业贷款",
                r"\\172.16.7.114\DataTeam\Products\小微贷款",
                r"\\172.16.7.114\DataTeam\Products\消费贷",
                r"\\172.16.7.114\DataTeam\Products\信用卡",
                r"\\172.16.7.114\DataTeam\Products\租赁资产",
                r"\\172.16.7.114\DataTeam\Products_log\银行间债券市场更新数据"
            ]
            for file_dir in file_dirs:
                for dirs in os.listdir(file_dir):
                    if str(dirs).split('资产')[0] in data['update_title']:
                        pathname=os.path.join(file_dir,dirs)
                        filename=data['update_title']+'.pdf'
                        # print(os.path.join(file_dir,dirs))
                        # print(filename)
                        # print(pathname)
                        # 遍历本地的产品，匹配到网络的，如果匹配到，则下载该产品目录下，即便了出现了关于也不影响啦啦
                        # while data['title'] in str(dirs).split('化')[0]:
                        print(data['update_title'] + "与" + dirs + "匹配")
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
                        try:
                            rep = requests.get(data.get('update_link'), headers=headers)
                            time.sleep(2)
                            with open(file_dir + r'\{}'.format(dirs) + r'\{}.pdf'.format(data['update_title']), 'wb') as f:
                                f.write(rep.content)
                                f.close()

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

                        with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\发行结果报告.xlsx','a') as f:
                            f.write(now_time)
                            f.write(data['update_title'])
                            f.write('\n')
                            f.close()

                        ftp_path='DealViewer\TrustAssociatedDoc'
                        folder='TrusteeReport'
                        for file in os.listdir(pathname):
                            if '.txt' in file:
                                TrustCode=file.split('.')[0]
                                # print(TrustCode)
                                ftp_path1=os.path.join(ftp_path,TrustCode)
                                # print(ftp_path1)
                                ftp_path2=os.path.join(ftp_path1,folder)
                                # print(ftp_path2)
                                sqlfilepath=ftp_path2+'/'
                                sqlfilepath=sqlfilepath.replace('\\','/')

                                remotepath = './'+filename
                                localpath = os.path.join(pathname, filename)
                                ftp_file='DealViewer\TrustAssociatedDoc{}'.format(TrustCode)
                                upload_file(remotepath,localpath,ftp_path2,TrustCode,sqlfilepath,filename,ftp_file)
                                # print(remotepath,localpath,ftp_path2,TrustCode,sqlfilepath,filename,ftp_file)

                                insert(TrustCode,sqlfilepath,filename,web_date)
                                # print(TrustCode)

        else:
            with open(r'D:\Work\DataTeam\Products_log\更新时间TXT记录\发行结果更新时间.txt', 'w') as f:
                f.write(first_update_times)
            sys.exit(0)






#上传FTP
def upload_file(remotepath, localpath, ftp_path2,TrustCode,sqlfilepath,filename,ftp_file):
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
        # ftp.mkd(ftp_path2)

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
            insert(TrustCode,sqlfilepath, filename)
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


#TrustAssociatedDocument表插入记录(DV页面显示)
def insert(TrustCode,sqlfilepath,filename,web_date):
    conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
                           database='PortfolioManagement', charset='utf8')
    b1=conn.cursor()
    try:
        select_Trustid="select Trustid from DV.view_Products where TrustCode='{}'".format(TrustCode)
        b1.execute(select_Trustid)
        Trust_id=b1.fetchone()[0]
        # print(select_Trustid)
        b2="insert into DV.TrustAssociatedDocument(Trustid,FileCategory,SubCategory,FilePath,FileName,FileType,Created,Creator) values({},'AnnouncementOfResults','NULL','{}',N'{}','pdf',GETDATE(),'py')".format(Trust_id,sqlfilepath,filename)
        # print(sqlfilepath)
        # print(filename)
        # print(b2)
        b1.execute(b2)
        conn.commit()
        print('记录插入成功')
        # print('\n')

        # insert_Interaction="insert into dbo.TrustAssociatedDocument_Interaction values({},'TrusteeReport',null,'{}'," \
        #                    "N'{}','pdf',{},'python')".format(Trust_id,sqlfilepath,filename,web_date)
        # b1.execute(insert_Interaction)
        # print('中间表记录插入完成！')
        # conn.commit()
        # b1.close()
        # conn.close()
    except:
        print(filename,'记录插入失败!')


if __name__ == '__main__':
    options = webdriver.ChromeOptions()
    options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"')
    driver = webdriver.Chrome(executable_path=r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe')

    use_selenium()
    # date()
    # update_pdf()
    driver.close()
    driver.quit()
        # sys.exit()
        # os._exit()
