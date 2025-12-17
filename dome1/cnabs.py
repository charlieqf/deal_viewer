import sys
import random
import time
import smtplib
from email.mime.text import MIMEText
import shutil
from selenium import webdriver
import pandas as pd
import pymssql
from pypinyin import lazy_pinyin
from selenium.webdriver.common.by import By
import warnings
from openpyxl import load_workbook

with open('last_updated_product', encoding='utf8') as f:
    lastProd = f.read()

qwe = pd.read_excel('product_list_20250412.xlsx', header=2)

namelist = [name.strip() for name in qwe.产品全称.tolist()]
# print(lastProd)
# print(namelist)
if lastProd not in namelist:
    print('上次最后的产品找不到')
    sys.exit()

conn = pymssql.connect(host='172.16.6.143\mssql', user='sa', password='PasswordGS2017',
                       database='PortfolioManagement', charset='utf8')
cur = conn.cursor()

data = pd.read_excel('product_list_20250412.xlsx')

CategoryCode = ['TrustStatus', 'AssetType', 'BasicAssetType', 'CollectionMethod', 'MarketCategory', 'RegulatoryOrg',
                'MarketPlace']

res = []
for c in CategoryCode:
    dic = {}
    sql = "select ItemTitle,ItemCode from dv.item where CategoryCode='{}'".format(c)
    cur.execute(sql)
    qe = cur.fetchall()
    for i in qe:
        dic[i[0]] = i[1]
    res.append(dic)

dic_TrustStatus, dic_AssetType, dic_BasicAssetType, dic_CollectionMethod, dic_MarketCategory, dic_RegulatoryOrg, dic_MarketPlace = res


def insert_db(i):
    TrustNameShort = data.loc[i][0]
    TrustCode = ''.join([i[0].upper() + i[1:] for i in lazy_pinyin(TrustNameShort)])
    TrustName = data.loc[i][1]

    IsTopUpAvailable = 1 if data.loc[i][8] == '是' else 0
    IssueAmount = data.loc[i][9] * 1e8

    try:
        TrustStatus = dic_TrustStatus[data.loc[i][2]]
    except:
        TrustStatus = 'null'

    TrustStartDate = "CONVERT(varchar, '{}', 101)".format(data.loc[i][21]) if len(data.loc[i][21]) == 10 else 'null'
    ClosureDate = "CONVERT(varchar, '{}', 101)".format(data.loc[i][22]) if len(data.loc[i][22]) == 10 else 'null'
    PoolCloseDate = "CONVERT(varchar, '{}', 101)".format(data.loc[i][26]) if len(data.loc[i][26]) == 10 else 'null'
    BookkeepingDate = "CONVERT(varchar, '{}', 101)".format(data.loc[i][27]) if len(data.loc[i][27]) == 10 else 'null'
    ListingDay = "CONVERT(varchar, '{}', 101)".format(data.loc[i][28]) if len(data.loc[i][28]) == 10 else 'null'

    sql_TrustId = "select max(TrustId)+1 from TrustManagement.Trust where TrustId<50000"
    cur.execute(sql_TrustId)
    TrustId = cur.fetchone()[0]

    sql_Trust = "SET IDENTITY_INSERT TrustManagement.Trust ON ;" \
                "insert into TrustManagement.Trust (TrustId,TrustCode, TrustName, TrustNameShort, " \
                "IssueAmount, PoolCloseDate, TrustStartDate, IsTopUpAvailable, ClosureDate, " \
                "IsMarketProduct, TrustStatus, BookkeepingDate, ListingDay) values" \
                "({},'{}', N'{}', N'{}', {}, {}, {}, {}, {}, 1, '{}', {}, {});" \
                "SET IDENTITY_INSERT TrustManagement.Trust OFF ;".format(
        TrustId, TrustCode, TrustName, TrustNameShort, IssueAmount, PoolCloseDate, TrustStartDate,
        IsTopUpAvailable, ClosureDate, TrustStatus, BookkeepingDate, ListingDay)

    cur.execute(sql_Trust)

    sql_AT = "SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust ON ;" \
             "insert into FixedIncomeSuite.Analysis.Trust(trustid ,trustcode,TrustName,startdate,EndDate,OfferAmount) select trustid ,trustcode,TrustName," \
             " truststartdate,closuredate,issueamount from TrustManagement.Trust where Trustid={};" \
             "SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust OFF ;".format(TrustId)
    cur.execute(sql_AT)

    MainUnderwriter = data.loc[i][31]
    ProductSubject = data.loc[i][36]
    Trustee = data.loc[i][37]
    RatingAgent = data.loc[i][39]
    ServiceInstitutions = data.loc[i][40]
    AccountingFirm = data.loc[i][41]
    EntrustOrganization = data.loc[i][45]
    TrusteeshipAuthority = data.loc[i][48]
    jgyh = data.loc[i][49]
    LegalCounsel = data.loc[i][42]
    TheActualFinancier = data.loc[i][53]

    sql_AdditionalInformation = "insert into DV.AdditionalInformation(TrustId,	AdditionalItemCode,	ChineseField,	AdditionalContent) values " \
                                "({}, 'MainUnderwriter', N'主承销商', N'{}')," \
                                "({}, 'ProductSubject', N'原始权益人\产品主体', N'{}')," \
                                "({}, 'Trustee', N'发行人\发行机构', N'{}')," \
                                "({}, 'RatingAgent', N'评级机构', N'{}')," \
                                "({}, 'ServiceInstitutions', N'资产服务机构/贷款服务机构', N'{}')," \
                                "({}, 'AccountingFirm', N'会计师事务所', N'{}')," \
                                "({}, 'EntrustOrganization', N'受托机构', N'{}')," \
                                "({}, 'TrusteeshipAuthority', N'托管机构', N'{}')," \
                                "({}, 'jgyh', N'监管银行', N'{}')," \
                                "({}, 'LegalCounsel', N'法律顾问', N'{}')," \
                                "({}, 'TheActualFinancier', N'实际融资人/归集主体', N'{}');".format(
        TrustId, MainUnderwriter, TrustId, ProductSubject, TrustId, Trustee, TrustId, RatingAgent, TrustId,
        ServiceInstitutions, TrustId, AccountingFirm, TrustId, EntrustOrganization,
        TrustId, TrusteeshipAuthority, TrustId, jgyh, TrustId, LegalCounsel, TrustId, TheActualFinancier)

    AssetType = data.loc[i][14]
    BasicAssetType = data.loc[i][15]
    CollectionMethod = data.loc[i][4]
    MarketCategory = data.loc[i][13]
    MarketPlace = data.loc[i][6]
    ProductSubject = data.loc[i][36]
    RegulatoryOrg = data.loc[i][38]
    TrustStartDate = data.loc[i][21]

    try:
        AssetType = dic_AssetType[AssetType]
    except:
        AssetType = 'null'

    try:
        BasicAssetType = dic_BasicAssetType[BasicAssetType]
    except:
        BasicAssetType = 'null'

    try:
        CollectionMethod = dic_CollectionMethod[CollectionMethod]
    except:
        CollectionMethod = 'null'

    try:
        MarketCategory = dic_MarketCategory[MarketCategory]
    except:
        MarketCategory = 'null'

    try:
        RegulatoryOrg = dic_RegulatoryOrg[RegulatoryOrg]
    except:
        RegulatoryOrg = 'null'

    try:
        MarketPlace = dic_MarketPlace[MarketPlace]
    except:
        MarketPlace = 'null'

    sql_TrustInfoExtension = "insert into TrustManagement.TrustInfoExtension (TrustId, StartDate, EndDate, ItemId, ItemCode, ItemValue) values " \
                             "({}, GETDATE(), null, null, 'AssetType',N'{}')," \
                             "({}, GETDATE(), null, null, 'BasicAssetType',N'{}')," \
                             "({}, GETDATE(), null, null, 'CollectionMethod', N'{}')," \
                             "({}, GETDATE(), null, null, 'MarketCategory', N'{}')," \
                             "({}, GETDATE(), null, null, 'MarketPlace', N'{}')," \
                             "({}, GETDATE(), null, null, 'ProductSubject', N'{}')," \
                             "({}, GETDATE(), null, null, 'RegulatoryOrg', N'{}')," \
                             "({}, GETDATE(), null, null, 'TrustStartDate', '{}');".format(
        TrustId, AssetType, TrustId, BasicAssetType, TrustId, CollectionMethod, TrustId, MarketCategory,
        TrustId, MarketPlace, TrustId, ProductSubject, TrustId, RegulatoryOrg, TrustId, TrustStartDate
    )

    TrustStartDate = data.loc[i][21]
    ClosureDate = data.loc[i][22]
    PoolCloseDate = data.loc[i][26]
    IsTopUpAvailable = "True" if data.loc[i][8] == '是' else 'False'
    sql_TrustExtension = "insert into TrustManagement.TrustExtension (TrustId, StartDate, ItemId, ItemCode, ItemValue) values " \
                         "({}, GETDATE(),1000, 'PoolCloseDate', '{}')," \
                         "({}, GETDATE(), 1002, 'ClosureDate', '{}')," \
                         "({}, GETDATE(), 9004, 'IsTopUpAvailable', '{}')," \
                         "({}, GETDATE(), 1001, 'TrustStartDate', '{}');".format(
        TrustId, PoolCloseDate, TrustId, ClosureDate, TrustId, IsTopUpAvailable, TrustId, TrustStartDate)
    try:
        cur.execute(sql_AdditionalInformation)
        cur.execute(sql_TrustInfoExtension)
        cur.execute(sql_TrustExtension)

    except Exception as e:
        print(e)
        print(sql_AdditionalInformation)
        print(sql_TrustInfoExtension)
        print(sql_TrustExtension)
        print(TrustCode)
    conn.commit()


newProds, errProds = [], []
for i in data.index[2:]:
    trustName = data.loc[i][1]
    if trustName == lastProd:
        print('新产品更新完毕')
        break
    sql = "select 1 from TrustManagement.Trust where TrustName=N'{}'".format(trustName)
    cur.execute(sql)
    try:
        if not cur.fetchone():
            print(trustName, 'begin')
            insert_db(i)
            newProds.append(trustName)
    except Exception as e:
        print(e)
        print('err0', trustName)
        errProds.append(trustName)
        continue

cur.execute("update DV.AdditionalInformation set AdditionalContent=null where AdditionalContent='nan'")
cur.execute("update TrustManagement.TrustInfoExtension set ItemValue=null where ItemValue='nan'")
conn.commit()

newLastProd = data.loc[2][1]
with open('last_updated_product', 'w', encoding='utf8') as f:
    f.write(newLastProd)

noFileProds = []
print(newProds)
print(errProds)

driver = webdriver.Chrome()
# 用户名：13666194865
# 密码：PasswordGS2021
s_url = 'https://www.cn-abs.com/product.html#/detail/basic-info?deal_id=7342'
driver.get(s_url)
driver.implicitly_wait(10)
# driver.find_element_by_xpath('//*[@id="user_name"]').send_keys('13666194865')
# driver.find_element_by_xpath('//*[@id="password"]').send_keys('Password01')
driver.find_element_by_xpath('//*[@id="root"]/div/div[2]/form/div[1]/div/div/span/div/div/span/input').send_keys(
    '13666194865')
driver.find_element_by_xpath('//*[@id="root"]/div/div[2]/form/div[2]/div/div/span/div/div/span/input').send_keys(
    'PasswordGS2021')

time.sleep(20)
# with open('产品入库.txt', encoding='utf8') as f:
#     l = f.readlines()
#     l = [i.split(' begin')[0] for i in l]

# for i in newProds:
#     driver.implicitly_wait(10)
#
#     # 输入全称
#
#     driver.find_element_by_xpath(
#         '//*[@id="root"]/div/div/div[1]/div/div[3]/div/div[3]/div/div/ul/li/div/span[1]/input').send_keys(i)
#     driver.implicitly_wait(10)
#
#     # 点击查询
#     driver.find_element_by_xpath(
#         '//*[@id="root"]/div/div/div[1]/div/div[3]/div/div[3]/div/div/ul/li/div/span[1]/span/i').click()
#     driver.implicitly_wait(10)
#     fz = 0
#     # 点击第一个产品
#     driver.find_element_by_xpath(
#         '//*[@id="root"]/div/div/div[2]/div/div/div[3]/div[2]/div/div/div[3]/div[1]/div[2]/div[2]/div/div[1]/div/div/div/div/div/div/div[2]/table/tbody/tr/td[2]/a').click()
#
#     driver.implicitly_wait(10)
#     # 点击下载
#     # print(fz,111)
#     while fz != 1:
#         try:
#             driver.switch_to.window(driver.window_handles[-1])
#             driver.find_element_by_css_selector(
#                 '#root > div > div > div.ant-layout > div.abs-product-page-title > div.abs-product-page-content > div.abs-product-page-content-title > div.abs-product-page-content-title-name')
#
#             fz = 1
#         except Exception as e:
#             driver.find_element_by_xpath(
#                 '//*[@id="root"]/div/div/div[2]/div/div/div[3]/div[2]/div/div/div[3]/div[1]/div[2]/div[2]/div/div[1]/div/div/div/div/div/div/div[2]/table/tbody/tr/td[2]/a').click()
#             driver.implicitly_wait(10)
#     # print(fz,222)
#     try:
#         driver.find_element_by_css_selector(
#             '#root > div > div > div.ant-layout > div.abs-product-page-title > div.abs-product-page-content > div.abs-product-page-content-title > div.abs-product-page-buttons > div:nth-child(2) > div.abs-btn > button').click()
#         print(i, '正常下载详细信息')
#
#     except Exception as e:
#         print(i, '下载异常')
#         noFileProds.append(i)
#         print(noFileProds)
#     time.sleep(random.uniform(1, 5))

for i in newProds:
    driver.implicitly_wait(10)

    # 输入全称

    driver.find_element_by_xpath(
        '//*[@id="root"]/div/div/div[1]/div/div[3]/div/div[3]/div/div/ul/li/div/span[1]/input').send_keys(i)
    driver.implicitly_wait(10)

    # 点击查询
    driver.find_element_by_xpath(
        '//*[@id="root"]/div/div/div[1]/div/div[3]/div/div[3]/div/div/ul/li/div/span[1]/span/i').click()
    driver.implicitly_wait(10)
    fz = 0
    time.sleep(3)
    driver.find_element(By.XPATH, '//div[text()="{}"]'.format(i)).click()
    driver.switch_to.window(driver.window_handles[-1])
    driver.implicitly_wait(10)

    try:
        # driver.find_element(By.XPATH, '//div[text()="下载")]').click()
        driver.find_element_by_xpath(
            '//*[@id="root"]/div/div/div[2]/div[1]/div[2]/div[1]/div[3]/div[2]/div/button').click()
        print(i, '正常下载详细信息')
    except Exception as e:
        print(i, '下载异常')
        noFileProds.append(i)
        print(noFileProds)

    time.sleep(random.uniform(1, 5))


def upLoad():
    toDate = time.strftime("%Y-%m-%d")
    newPath = r'\\172.16.7.114\DataTeam\交易所产品信息下载\{}'.format(toDate)
    oriPath = r'E:\脚本\日常任务-产品列表'
    shutil.copytree(oriPath, newPath)
    print('文件夹传输完毕')


def mail():
    if any((newProds, noFileProds, errProds)):
        msg_from = 'fengyanyan@goldenstand.cn'  # 发送方邮箱
        passwd = 'Fyy2516302813'  # 填入发送方邮箱的授权码
        msg_to1 = ['zhangtongyao@gdsd.wecom.work', 'fengyanyan@goldenstand.cn']

        content = '{}\n'.format(time.strftime("%Y-%m-%d")) + '\n'
        if newProds:
            content += '企业资产资产证券化，新增产品 ' + str(len(newProds)) + '个：' + '\n'
            for i in newProds:
                cur.execute("select trustid from TrustManagement.Trust where trustname=N'{}'".format(i))
                id = cur.fetchone()[0]
                content += str(id)
                content += ' '
                content += i
                content += '\n'
        content += '\n'
        if noFileProds:
            content += '其中不能获取产品详细信息的产品 ' + str(len(noFileProds)) + '个：' + '\n'
            for i in noFileProds:
                content += i
                content += '\n'
        content += '\n'
        if errProds:
            content += '新建产品异常 ' + str(len(errProds)) + '个，需核查：' + '\n'
            for i in errProds:
                content += i
                content += '\n'

        for msg_to in msg_to1:
            subject = "企业资产证券化入库明细"  # 主题
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
    else:
        print('CNABS无产品更新')


upLoad()
mail()
