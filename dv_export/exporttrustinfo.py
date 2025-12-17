import math
from xml.etree.ElementTree import Element,tostring
from xml.dom import minidom
from xml.etree import ElementTree
import dbhelper as dbh
import pandas as pd
import html
from urllib import parse
import json
import sys
TABLES = "tables"
TABLE = "table"
NAME = "name"
PK = "pk"
FK = "fk"
FTABLE = "ftable"
COLUMN = "column"
TRUSTID = "TrustId"
TRUSTCODE = "TrustCode"
DATA = "data"
ENCODE = "encode"
ENCODE_LESSTHAN = "encodeLessThan"
XML = "xml"
TYPE = "type"
VALUE = "value"
VALUES = "values"
SELFCOLUMN = "selfcolumn"
DEPENDENCES = "dependences"
DEPENDENCE = "dependence"
TABLENAME = "tablename"
UNIQUE = "unique"
INSERTED = "inserted"
ROOTTABLE = "TrustManagement.Trust"
PROC_NAME = "dbo.usp_OutputData"
ISINSERTED = "Y"
ISUNIQUE = "Y"
CURRENT = "current"
ITEMID = "ItemId"
SysNull = "System.DBNull"
DBNULL = "DBNULL"
FKS = "fks"
DEFAULT = "default"
DIVISOR = 2
USER_TABLE = "dbo.UserOperation"
EVENTTYPE = "eventType"
TITLE = "title"
EVENTDESC = "eventDesc"
CalendarDataSourceNAME = "ProductManage.tblCalendarDataSource"
BusinessAssociationNAME = "QuickWizard.QuickWizard.BusinessAssociation"
DataStoreNAME = "QuickWizard.QuickWizard.DataStore"
SESSIONID = "SessionId"
ItemAliasTableName = "ProductManage.ItemAlias"
ItemTableName = "ProductManage.Item"
ItemCategoryTableName = "ProductManage.ItemCategory"
TrustPaymentScenarioNAME = "ProductManage.tblTrustPaymentScenario"
LESS_THAN = "LeSs_ThAn"
ENCODECOLUMN = "EnCode"
SepiSepecialchars = {'""',"'","<",">","&" }
TRUSTMANAGEMENT = "PortfolioManagement"
TASKPROCESS = "TaskProcess"

def prettify(elem):
    """
    Return a pretty-printed XML string for the Element.
    """
    rough_string = ElementTree.tostring(elem,'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")
def read_xml(in_path):
    '''''读取并解析xml文件
       in_path: xml路径
       return: ElementTree'''
    tree = ElementTree.ElementTree()
    tree.parse(in_path)
    return tree

def write_xml(tree, out_path):
    '''''将xml文件写出
       tree: xml树
       out_path: 写出路径'''
    tree.write(out_path, encoding="utf-8", xml_declaration=True)

def if_match(node, kv_map):
    '''''判断某个节点是否包含所有传入参数属性
       node: 节点
       kv_map: 属性及属性值组成的map'''
    for key in kv_map:
        if node.get(key) != kv_map.get(key):
            return False
    return True

# ----------------search -----------------
def find_nodes(tree, path):
    '''''查找某个路径匹配的所有节点
       tree: xml树
       path: 节点路径'''
    return tree.findall(path)

def get_node_by_keyvalue(nodelist, kv_map):
    '''''根据属性及属性值定位符合的节点，返回节点
       nodelist: 节点列表
       kv_map: 匹配属性及属性值map'''
    result_nodes = []
    for node in nodelist:
        if if_match(node, kv_map):
            result_nodes.append(node)
    return result_nodes

# ---------------change ----------------------
def change_node_properties(nodelist, kv_map, is_delete=False):
    '''修改/增加 /删除 节点的属性及属性值
       nodelist: 节点列表
       kv_map:属性及属性值map'''
    for node in nodelist:
        for key in kv_map:
            if is_delete:
                if key in node.attrib:
                    del node.attrib[key]
            else:
                node.set(key, kv_map.get(key))

def change_node_text(nodelist, text, is_add=False, is_delete=False):
    '''''改变/增加/删除一个节点的文本
       nodelist:节点列表
       text : 更新后的文本'''
    for node in nodelist:
        if is_add:
            node.text += text
        elif is_delete:
            node.text = ""
        else:
            node.text = text

def create_node(tag, property_map, content):
    '''新造一个节点
       tag:节点标签
       property_map:属性及属性值map
       content: 节点闭合标签里的文本内容
       return 新节点'''
    element = Element(tag, property_map)
    element.text = content
    return element

def add_child_node(nodelist, element):
    '''''给一个节点添加子节点
       nodelist: 节点列表
       element: 子节点'''
    for node in nodelist:
        node.append(element)


def del_node_by_tagkeyvalue(nodelist, tag, kv_map):
    '''''同过属性及属性值定位一个节点，并删除之
       nodelist: 父节点列表
       tag:子节点标签
       kv_map: 属性及属性值列表'''
    for parent_node in nodelist:
        children = parent_node.getchildren()
       
        for child in children:
            if child.tag == tag and if_match(child, kv_map):
                parent_node.remove(child)

def get_element_name(node):
    if(node.find(NAME) is not None):
        return node.find(NAME).text
    else:
        return ""

def get_dbname_by_tablename(tablename):
    dbname = TRUSTMANAGEMENT
    if TASKPROCESS.upper() in tablename.upper():
        dbname = TASKPROCESS
    return dbname

def find_tableelement_by_namevalue(tableElement,tableName,tree):
    for element in find_nodes(tree,TABLE):
        if tableName == get_element_name(element):
            return element

def find_fkelement_by_name(ftableElement,columnName):
    pkElement = find_nodes(ftableElement,PK)[0]
    if columnName == get_element_name(pkElement):
        return pkElement
    else:
        dataElement = find_nodes(ftableElement,DATA)[0]
        if dataElement is None:
            return None
        for columnElement  in  find_nodes(dataElement,columnName):
            if columnName == get_element_name(columnElement):
                return find_nodes(columnElement,VALUES)[0]
        return None
   

def get_fkvalueset_by_fkelement(tableElement,fkElement,tree):
    valueSet = []
    for ftableEle in tableElement.iter(FTABLE):
        ftableElement = find_tableelement_by_namevalue(tableElement,ftableEle.text,tree)
        fkEleOfFable = find_fkelement_by_name(ftableElement,ftableEle.attrib.get(COLUMN))
        #print(fkEleOfFable)
        if fkEleOfFable is None:
            continue
        #print(len(find_nodes(fkEleOfFable,VALUE)))
       # print(ElementTree.tostring(fkEleOfFable))
        #print(ElementTree.tostring(find_nodes(fkEleOfFable,VALUE)[0]))
        if len(find_nodes(fkEleOfFable,VALUE)) > 0:
            valuesElement = find_nodes(fkEleOfFable,VALUE)[0]
            
            #for columnValue in valuesElement:
            #print(valuesElement.text)
            if valuesElement.text is not None and DBNULL != valuesElement.text:
                valueSet.append(valuesElement.text)
    #tableElement = None
    return valueSet
def split(list,parts):
    '''''将条件 A in (1,2,3,...) And B in (1,2,3,...) 拆分为： A in（1,2） And B in (1,2);A in（1,2） And B in (3);A in（1,2） And B in (3);A in（3） And B in (1,2);
        A in（3） And B in (3)'''
    i = 0
    getcount = math.ceil(len(list)/parts)
    loc = 0
    tmpset = {}
    while i < parts:
        if len(list)<parts:
            tmpset.update({0:list})
        else:
            if loc <= len(list):
                if len(list[loc:loc+getcount]) != 0:
                    tmpset.update({i:list[loc:loc+getcount]})
                    loc = loc + getcount
        i = i + 1
    return tmpset
def incisecore(dictionary,factor):
    resultList = []
    conditionStr = ""
    condiListList = []
    conditionDictionary = {}
    currentIndexList = []
    conditionSplitCount = []
    for fKey in dictionary.keys():
        conditionDictionary.update({fKey:split(dictionary[fKey],factor)})
        currentIndexList.append(len(conditionDictionary[fKey])-1)
        conditionSplitCount.append(len(conditionDictionary[fKey])-1)
    
    while True:
        i = 0
        conditionStrList = []
        for fKey in conditionDictionary.keys():
            setEnumerable = conditionDictionary[fKey][currentIndexList[i]]
            conditionStr = fKey + " in (''" + "'',''".join(str(j) for j in setEnumerable) + "'')"
            conditionStrList.append(conditionStr)
            i = i + 1
        condiListList.append(conditionStrList)
        if AllIsZero(currentIndexList):
            break
        flag = True
        for j in range(len(currentIndexList)):
           
            if currentIndexList[j] > 0 and flag:
                currentIndexList[j] = currentIndexList[j] - 1
                flag = False
            elif currentIndexList[j] == 0 and flag:
                currentIndexList[j] = conditionSplitCount[j]
    for conditionLst in condiListList:
        conditionStr = " and ".join(conditionLst)
        resultList.append(conditionStr)
    return resultList

def AllIsZero(lst):
    for v in lst:
        
        if v != 0:
            return False
    return True        

def incisecondition(dictionary):
    conditionStrList = []
    if len(dictionary) == 0:
        return conditionStrList
    fkvalueCount = 0
    #print(dictionary)
    #dictionary.update({"trustidsss":[11111,22222,33333,44444]})
    for fk in dictionary:
        fkvalueCount = fkvalueCount + len(dictionary[fk])
    factor = math.ceil(fkvalueCount/DIVISOR)
    conditionStrList = incisecore(dictionary, factor)
    return conditionStrList

def exec_handle_fkvalue(dataSet,tableName,pk,condition):
    if condition is None:
        return None
    LoadData ="SET NOCOUNT ON;exec dbo.usp_OutputData_Export '{0}','{1}','{2}'".format(tableName,pk,condition) 
    dbcn_FixedIncomeSuite = dbh.open_dbconn("PortfolioManagement")
    df = dbh.exec_fetch_dataset(LoadData,dbcn=dbcn_FixedIncomeSuite)
    #print(df[0][0])
    return df 
def assembleaataxml(tableElement,dataSet):
    if len(dataSet)== 0:
        return
    messageCollection = []
    columnCollection = []
    dataCollection = []
    pkCollection = []
    #print('bbbbbbbbbbbbbbbb',len(dataSet[0]))
    if len(dataSet[0]) == 1:
        messageCollection = dataSet[1][0]
    elif len(dataSet[0]) == 3:
        messageCollection = dataSet[1][0]
        columnCollection = dataSet[1][1]
        dataCollection = dataSet[1][2]
    elif len(dataSet[0]) == 2:
        messageCollection = dataSet[1][0]
        columnCollection = dataSet[1][1]
    else:
        messageCollection = dataSet[1][0]
        columnCollection = dataSet[1][1]
        dataCollection = dataSet[1][2]
        pkCollection = dataSet[1][3]
    if len(pkCollection) != 0:
        for pk in pkCollection:
            newnode = create_node("value",{}, str(pk[0]))
            find_nodes(tableElement,PK)[0].append(newnode)
    if len(columnCollection) == 0 or len(dataCollection) == 0 or len(tableElement) == 0:
        return
    dataElement = Element(DATA) 
    for column in columnCollection:
        newnode = Element('column')
        namenode =  Element('name')
        namenode.text = column[0]
        typenode =  Element('type')
        typenode.text  = column[1]
        values = Element('values')
        newnode.append(namenode)
        newnode.append(typenode)
        newnode.append(values)
        dataElement.append(newnode)
    columnList = find_nodes(dataElement,COLUMN)
    for dataRow in dataCollection:
        for i in range(len(columnList)):
            value = ""
            obj = dataRow[i]
            if obj is None:
                value = DBNULL
            else:
                value = str(obj)
            #print(find_nodes(columnList[i],TYPE)[0].text)
            if find_nodes(columnList[i],TYPE)[0].text != XML:
                if str(dataRow[i]).startswith('<') or str(dataRow[i]).endswith('/>'):
                     byte = bytes(str(dataRow[i]))
                     value = str(byte)
                     find_nodes(columnList[i],TYPE)[0].text = ENCODE
                elif "<" in str(dataRow[i]):
                     value = value.replace('<',LESS_THAN)
                     find_nodes(columnList[i],TYPE)[0].text = ENCODE_LESSTHAN
            valuenode = Element('value')
            valuenode.text = html.unescape(value)
            find_nodes(columnList[i],VALUES)[0].append(valuenode)
    #print(html.unescape(str(ElementTree.tostring(find_nodes(tableElement,DATA)[0]))))
    if len(find_nodes(tableElement,DATA)) > 0:
        delnode = find_nodes(tableElement,DATA)[0]
        tableElement.remove(delnode)
    tableElement.append(dataElement)
    return tableElement
def updateexportstatus(trustId,status):
    dbcn_FixedIncomeSuite = dbh.open_dbconn("TaskProcess")
    selectsql = "select count(0) From TASKPROCESS.dbo.TrustExportStatus where TrustId = {0}".format(trustId)
    isExist = dbh.exec_commit_with_result(selectsql,dbcn=dbcn_FixedIncomeSuite)
    if isExist == 0:
        insertSql = "insert into TASKPROCESS.dbo.TrustExportStatus(TrustId,Status) values({0}, {1})".format(trustId,status)
        dbh.exec_commit(insertSql,dbcn=dbcn_FixedIncomeSuite)
    else:
        updatesql = "update TASKPROCESS.dbo.TrustExportStatus set Status = {0} where TrustId = {1}".format(status,trustId)
        dbh.exec_commit(updatesql,dbcn=dbcn_FixedIncomeSuite)
def querydatafromdb(tableElement,trustid,tree):
    '''从数据库获取数据填充xml节点
       tableElement : 需要填充的XML节点
       trustid : 产品ID
       tree ：XML完整节点
       '''
    tableName = ""
    pk = ""
    fk = ""
    fkValue = ""
    fkDictionary = {}
    #print(ElementTree.tostring(tableElement))
    if ROOTTABLE == get_element_name(tableElement):
        #print(get_element_name(tableElement),ROOTTABLE)
        tableName = ROOTTABLE
        pk = TRUSTID
        tmpset = []
        tmpset.append(trustid)
        fkDictionary.update({TRUSTID:tmpset})
    else:
         tableName = get_element_name(tableElement)
        
         pk = get_element_name(find_nodes(tableElement,PK)[0])
         if pk is None:
             pk = ""
         #print(ElementTree.tostring(tableElement))
         for fkElement in find_nodes(find_nodes(tableElement,FKS)[0],FK):
             fkset = set()
             selfColumn = find_nodes(fkElement,SELFCOLUMN)[0]
             fk = selfColumn.text
             tmpset = []
             if selfColumn.attrib.get(DEFAULT) is not None :
                 fkValue = selfColumn.attrib.get(DEFAULT)
                 fkset.add(fkValue)
                 fkDictionary.update({fk:fkValue})
             else:
                 xElement = fkElement
                 tmpset = get_fkvalueset_by_fkelement(tableElement,xElement,tree)
                 #print('aaaa',tmpset)
                 if len(tmpset) ==0:
                     continue
                 fkDictionary.update({fk:tmpset})
                # print(tableElement)
    
             
    dataSet = []
    conditionStrList = incisecondition(fkDictionary)
    for condition in conditionStrList:
        dataSet = exec_handle_fkvalue(dataSet,tableName,pk,condition)
    tableElement = assembleaataxml(tableElement,dataSet)
    return tableElement
         #print(ElementTree.tostring(find_nodes(tree,PK)[0]))

def ExportTrustInfo(filePath,TrustId):
     '''''导出产品
          filePath ：文件夹
          fileName ：文件名
          TrustId : 产品ID
     '''
     tree = read_xml(filePath)
     updateexportstatus(TrustId,0)
     for tableElement in find_nodes(tree,TABLE):
        #print(get_dbname_by_tablename(get_element_name(tableElement)))
        #dbName = get_dbname_by_tablename(get_element_name(tableElement))
        #dbcn = dbh.open_dbconn(dbName)
        #print(ElementTree.tostring(tableElement))
        tableElement = querydatafromdb(tableElement,TrustId,tree)
     updateexportstatus(TrustId,1)
     saveFilePaht=r'E:\TSSWCFServices\FileBaseFolder\ExportFiles\TrustInfo'
     write_xml(tree, filePath.replace('.xml','_'+str(TrustId)+'.xml'))
if __name__ == "__main__":
    # if len(sys.argv) < 2 or len(sys.argv[1]) < 1:
    #     raise Exception("*ERROR*脚本参数未指定，程序退出")
    # ags=json.loads(parse.unquote_plus(sys.argv[1]))
    TrustId = 26065
    filePath = r'E:\TSSWCFServices\FileBaseFolder\ExportFiles\TrustInfo\QuickDeal_TrustInfoImportAndExportModel.xml'
    ExportTrustInfo(filePath,TrustId)
    
    
    
    
   