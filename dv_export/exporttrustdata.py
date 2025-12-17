#import gsquantlib.srvpatch.chts
# coding=utf-8
import sys
import warnings
import numpy as np
import pandas as pd
import os
import zipfile

from gsquantlib import gsdts,gsapi
import gsquantlib.utils.display as gsdisp
from exporttrustinfo import ExportTrustInfo


warnings.filterwarnings("ignore")
pd.options.display.float_format = '{:.2f}'.format

trustId =sys.argv[1] #26065
save_directory = r'E:\TSSWCFServices\FileBaseFolder\QuantStudio\Running\08411afc-a4da-4ee8-b008-63a353490c4d\AnalysisModel\-1'
# 封包日归集现金流 [DV].[PaymentScheduleAggregation_Get]
#起始日期	结束日期	本金	利息	总现金流
#StartDate	EndDate	Principal	Interest	TotalCashflow
def getPaymentScheduleAggregation():

    columnNames ={'StartDate':['StartDate'], 
    'EndDate':['EndDate'], 
    'Principal':['Principal'], 
    'Interest':['Interest'], 
    'TotalCashflow':['TotalCashflow']}
    df0 = pd.DataFrame(columnNames) 

    aggregationData=gsdts.load_data("PortfolioManagement.DV.PaymentScheduleAggregation_Get", trustId,0, direct=1)
    if len(aggregationData)==0: 
          tempData = {'StartDate': [], 'EndDate': [],'PrincipalAmount':[],'InterestAmount':[],'TotalAmount':[]}          
          aggregationData = pd.DataFrame(tempData) 
    #print(aggregationData)
    df1 = pd.DataFrame(aggregationData)   
    df1 = {
    'StartDate': df1.pop('StartDate'),
    'EndDate': df1.pop('EndDate'),
    'Principal': df1.pop('PrincipalAmount'),
    'Interest': df1.pop('InterestAmount'),
    'TotalCashflow': df1.pop('TotalAmount')
    }
    df1 = pd.DataFrame(df1)
    df1['StartDate'] = [''] * len(df1['StartDate'])

    result = pd.concat([df0, df1])
    result = {
    '起始日期': result.pop('StartDate'),  
    '结束日期': result.pop('EndDate'),
    '本金': result.pop('Principal'),
    '利息': result.pop('Interest'),
    '总现金流': result.pop('TotalCashflow')  
    }
    result = pd.DataFrame(result)    
    #print(result)   
    
    excel_file1 = os.path.join(save_directory, "封包日归集现金流_%s.xlsx"%(trustId))
    result.to_excel(excel_file1, index=False)
  

#存续期归集现金流  
#起始日期	结束日期	应付本金	应付利息	实付本金	实付利息	循环购买	累计损失金额	累计违约金额	期初余额
#StartDate	EndDate	PrincipalDue	InterestDue	PrincipalPaid	InterestPaid	RevolvingPurchase	CumulativeDefault	CumulativeLoss	OpeningBalance

def getPoolCashflow():
    
    columnNames ={'StartDate':['StartDate'], 
    'EndDate':['EndDate'], 
    'PrincipalDue':['PrincipalDue'], 
    'InterestDue':['InterestDue'], 
    'PrincipalPaid':['PrincipalPaid'],
    'InterestPaid':['InterestPaid'],
    'RevolvingPurchase':['RevolvingPurchase'],
    'CumulativeDefault':['CumulativeDefault'],
    'CumulativeLoss':['CumulativeLoss'],
    'OpeningBalance':['OpeningBalance']    
    }
    df0 = pd.DataFrame(columnNames)

    df1=gsdts.load_data("PortfolioManagement.DV.PoolCashflowHistory_Get", trustId, direct=1)    

    result = pd.concat([df0, df1])    
    result = {
    '起始日期': result.pop('StartDate'),  
    '结束日期': result.pop('EndDate'),
    '应付本金': result.pop('PrincipalDue'),
    '应付利息': result.pop('InterestDue'),
    '实付本金': result.pop('PrincipalPaid') ,
    '实付利息': result.pop('InterestPaid'),
    '循环购买': result.pop('RevolvingPurchase'),
    '累计损失金额': result.pop('CumulativeDefault'),
    '累计违约金额': result.pop('CumulativeLoss') ,
    '期初余额': result.pop('OpeningBalance')
    }
    result = pd.DataFrame(result)
    #print(result)
    excel_file1 = os.path.join(save_directory, "存续期归集现金流_%s.xlsx"%(trustId))
    result.to_excel(excel_file1, index=False) 

#证券分配历史
def getFactBondPayment():

    df1=gsdts.load_data("PortfolioManagement.DV.FactBondPayment_Get_Export", trustId, direct=1)
    excel_file1 = os.path.join(save_directory, "证券分配历史_%s.xlsx"%(trustId))
    df1.to_excel(excel_file1, index=False)


getPaymentScheduleAggregation()
getFactBondPayment()
getPoolCashflow()
ExportTrustInfo(r'E:\TSSWCFServices\FileBaseFolder\QuantStudio\Running\08411afc-a4da-4ee8-b008-63a353490c4d\AnalysisModel\-1\QuickDeal_TrustInfoImportAndExportModel.xml',trustId)

# 文件压缩用于下载
fileCount=0
zip_file_name = "TrustInfo_%s.zip"%(trustId)
full_zip_path = os.path.join(r'E:\TSSWCFServices\FileBaseFolder\ExportFiles\TrustInfoZip', zip_file_name)
with zipfile.ZipFile(full_zip_path, 'w') as zipf:
    for file in os.listdir(save_directory):
        file_path = os.path.join(save_directory, file)
        if os.path.isfile(file_path) and str(trustId) in file:            
            fileCount+=1
            zipf.write(file_path, arcname=file)
print(fileCount)