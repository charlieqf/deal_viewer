#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cnabs_import_bonds.py - 将cnabs_securities.xlsx导入到数据库TrustManagement_TrustBond表

功能：
1. 读取cnabs_securities.xlsx
2. 检查Excel中A列（产品全称）与TrustManagement_Trust表中TrustName的一对一匹配
3. 导入数据到TrustManagement_TrustBond表

使用方法：
    python cnabs_import_bonds.py                    # 默认读取cnabs_securities.xlsx
    python cnabs_import_bonds.py your_file.xlsx    # 指定Excel文件
    python cnabs_import_bonds.py --check-only      # 仅检查匹配，不导入
"""

import os
import sys
import pandas as pd
import pyodbc
from datetime import datetime


# ==================== 配置 ====================
DEFAULT_EXCEL = "cnabs_securities.xlsx"

# excel字段
'''
产品全称	证券名称	证券代码	发行量	发行量(万元)	还本方式	类型	发行利率	发行利率(%)	利率形式	当前利率	当前利率(%)	预计到期日	联合资信（原始）	联合资信（当前）	中诚信国际（原始）	中诚信国际（当前）	大公国际（原始）	大公国际（当前）	东方金诚（原始）	东方金诚（当前）	鹏元资信（原始）	鹏元资信（当前）	新世纪评级（原始）	新世纪评级（当前）	联合信用（原始）	联合信用（当前）	评级机构1（原始）	评级机构1（当前）
深圳能源集团股份有限公司2024年度新能源1号第一期绿色定向资产支持商业票据	24深圳新能1ABN001优先(绿色)	082400476.IB	800000000	80,000.00	到期还本	优先级	0.0215	2.15%	固定	0.0215	2.15%	2024-10-25	AAA	AAA														
深圳能源集团股份有限公司2024年度新能源1号第一期绿色定向资产支持商业票据	24深圳新能1ABN001次(绿色)	082400477.IB	35440000	3,544.00	到期还本	次级	0	0.00%		0	0.00%	2029-03-07	NR	NR														
京东科技荟臻2号第1期资产支持专项计划	荟臻021A	135572.SZ	1167000000	116,700.00	过手	优先级	0.0278	2.78%	固定	0.0278	2.78%	2024-09-30			AAA	AAA												
京东科技荟臻2号第1期资产支持专项计划	荟臻021B	135573.SZ	63000000	6,300.00	过手	夹层级	0.031	3.1%	固定	0.031	3.1%	2024-09-30			AA+	AA+												
京东科技荟臻2号第1期资产支持专项计划	荟臻021C	135574.SZ	76000000	7,600.00	过手	夹层级	0.057	5.7%	固定	0.057	5.7%	2024-09-30			A-	A-												
京东科技荟臻2号第1期资产支持专项计划	荟臻021D	135575.SZ	84000000	8,400.00	过手	次级	0.05	5%		0.05	5%	2024-09-30			NR	NR												
君创国际融资租赁有限公司2023年度第二期普惠定向资产支持票据(债券通)	23君创普惠ABN002BC优先A	082320001.IB	1370000000	137,000.00	过手	优先级	0.053	5.3%	浮动	0.0545	5.45%	2025-02-28	AAA	AAA														
君创国际融资租赁有限公司2023年度第二期普惠定向资产支持票据(债券通)	23君创普惠ABN002BC优先B	082320002.IB	140000000	14,000.00	过手	夹层级	0.061	6.1%	固定	0.061	6.1%	2025-05-28	AA+	AA+														
君创国际融资租赁有限公司2023年度第二期普惠定向资产支持票据(债券通)	23君创普惠ABN002BC优先C	082320003.IB	100000000	10,000.00	过手	夹层级	0.063	6.3%	固定	0.063	6.3%	2025-07-28	AA	AA														
君创国际融资租赁有限公司2023年度第二期普惠定向资产支持票据(债券通)	23君创普惠ABN002BC次	082320004.IB	146830000	14,683.00	过手	次级	0	0.00%		0	0.00%	2026-01-28	NR	NR														
'''
# 有多家评级机构的评级数据字段但是最多只有一家机构有值，对应到OriginalCreditRating和ClassName

# 数据库连接配置
DB_CONFIG = {
    "Driver": "{ODBC Driver 18 for SQL Server}",
    "Server": "113.125.202.171,52482",
    "Database": "PortfolioManagement",
    "UID": "sa",
    "PWD": "PasswordGS2017",
    "Encrypt": "no",
    "TrustServerCertificate": "yes",
}


def get_sql_connection():
    """获取数据库连接"""
    try:
        conn_str = ";".join([f"{k}={v}" for k, v in DB_CONFIG.items()])
        conn = pyodbc.connect(conn_str)
        print("✓ 数据库连接成功")
        return conn
    except pyodbc.Error as e:
        print(f"✗ 数据库连接失败: {e}")
        return None


def read_excel(file_path):
    """读取Excel文件的'证券信息'sheet"""
    if not os.path.exists(file_path):
        print(f"✗ 文件不存在: {file_path}")
        return None
    
    try:
        df = pd.read_excel(file_path, sheet_name='证券信息', engine='openpyxl')
        print(f"✓ 读取Excel成功: {file_path} (sheet: 证券信息)")
        print(f"  行数: {len(df)}")
        print(f"  列名: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"✗ 读取Excel失败: {e}")
        return None


def get_trust_mapping(conn):
    """
    从TrustManagement_Trust表获取TrustName到TrustId的映射
    返回: {TrustName: TrustId}
    """
    cursor = conn.cursor()
    query = "SELECT TrustId, TrustName FROM TrustManagement.Trust"
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        mapping = {row.TrustName: row.TrustId for row in rows}
        print(f"✓ 获取Trust映射成功，共 {len(mapping)} 条记录")
        return mapping
    except Exception as e:
        print(f"✗ 获取Trust映射失败: {e}")
        return {}


def check_product_matching(df, trust_mapping, conn):
    """
    检查Excel中产品全称与数据库TrustName的一对一匹配
    
    返回:
        matched: [(产品全称, TrustId), ...]  匹配成功的
        unmatched: [产品全称, ...]  未匹配的
        duplicates: {产品全称: count}  Excel中重复的产品
    """
    print("\n" + "=" * 80)
    print("产品匹配检查")
    print("=" * 80)
    
    # 获取Excel中所有唯一的产品全称
    product_names = df["产品全称"].unique().tolist()
    print(f"Excel中唯一产品数: {len(product_names)}")
    
    # 检查Excel中是否有重复产品（同一产品多个证券是正常的）
    product_counts = df["产品全称"].value_counts()
    print(f"Excel中总行数（证券数）: {len(df)}")
    
    matched = []
    unmatched = []
    
    for product_name in product_names:
        if product_name in trust_mapping:
            matched.append((product_name, trust_mapping[product_name]))
        else:
            unmatched.append(product_name)
    
    # 查询数据库中每个TrustId的现有证券数和设立日
    db_bond_counts = {}
    db_start_dates = {}  # TrustId -> TrustStartDate
    if matched:
        try:
            cursor = conn.cursor()
            trust_ids_str = ",".join([str(m[1]) for m in matched])
            
            # 查询证券数
            query = f"""
                SELECT TrustId, COUNT(DISTINCT TrustBondId) as BondCount
                FROM TrustManagement.TrustBond 
                WHERE TrustId IN ({trust_ids_str})
                GROUP BY TrustId
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                db_bond_counts[row[0]] = row[1]
            
            # 查询设立日
            query = f"""
                SELECT TrustId, ItemValue
                FROM TrustManagement.TrustExtension 
                WHERE TrustId IN ({trust_ids_str}) AND ItemCode = 'TrustStartDate'
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                db_start_dates[row[0]] = row[1]
        except Exception as e:
            print(f"查询数据库失败: {e}")
    
    # 打印匹配结果
    print(f"\n匹配成功: {len(matched)} 个产品")
    print(f"未匹配: {len(unmatched)} 个产品")
    
    if unmatched:
        print("\n--- 未匹配的产品列表 ---")
        for i, name in enumerate(unmatched, 1):
            print(f"  {i}. {name}")
    
    # 检查缺少设立日的产品
    missing_start_date = []
    if matched:
        print("\n--- 匹配成功的产品列表 ---")
        has_existing = False
        for i, (name, trust_id) in enumerate(matched, 1):
            excel_count = len(df[df["产品全称"] == name])
            db_count = db_bond_counts.get(trust_id, 0)
            start_date = db_start_dates.get(trust_id)
            
            # 构建状态标记
            warnings = []
            if db_count > 0:
                warnings.append("已有数据")
                has_existing = True
            if not start_date:
                warnings.append("无设立日")
                missing_start_date.append((name, trust_id))
            
            warning_str = " ⚠️ " + ", ".join(warnings) if warnings else ""
            print(f"  {i}. {name} -> TrustId={trust_id} (Excel: {excel_count}, DB: {db_count}){warning_str}")
        
        if has_existing:
            print("\n⚠️ 警告: 部分产品在数据库中已有证券数据，导入时会先删除再插入")
        
        if missing_start_date:
            print(f"\n❌ 错误: {len(missing_start_date)} 个产品缺少设立日(TrustStartDate)，无法继续导入:")
            for name, trust_id in missing_start_date:
                print(f"     - {name} (TrustId={trust_id})")
    
    return matched, unmatched, missing_start_date, db_start_dates

def import_bonds(conn, df, matched, db_start_dates):
    """
    将证券数据导入TrustManagement_TrustBond表
    
    TrustBond表结构（需要根据实际表结构调整）：
        CREATE TABLE [TrustManagement].[TrustBond](
            [TrustBondId] [int] NOT NULL,
            [TrustId] [int] NOT NULL,
            [StartDate] [date] NOT NULL,
            [EndDate] [date] NULL,
            [ItemId] [int] NOT NULL,
            [ItemCode] [nvarchar](255) NULL,
            [ItemValue] [nvarchar](max) NULL,
        CONSTRAINT [PK_TrustBond] PRIMARY KEY CLUSTERED 
        (
            [TrustBondId] ASC,
            [TrustId] ASC,
            [StartDate] ASC,
            [ItemId] ASC
        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]


    select *
    from [TrustManagement].[TrustBond]
    where TrustID = 25067;

TrustBondId	TrustId	StartDate	EndDate	ItemId	ItemCode	ItemValue
0	25067	2021-08-12	NULL	2000	ClassName	AAA
0	25067	2021-08-12	NULL	2001	IssueDate	2021-06-17
0	25067	2021-08-12	NULL	2002	CurrencyOfIssuance	CNY
0	25067	2021-08-12	NULL	2003	OfferAmount	530000000
0	25067	2021-08-12	NULL	2009	CouponBasis	3.45
0	25067	2021-08-12	NULL	2011	CouponPaymentReference	固定利率
0	25067	2021-08-12	NULL	2014	Denomination	100
0	25067	2021-08-12	NULL	2015	LegalMaturityDate	2022-06-26
0	25067	2021-08-12	NULL	2017	OriginalCreditRating	AAA
0	25067	2021-08-12	NULL	2019	PaymentConvention	过手摊还
0	25067	2021-08-12	NULL	2029	ShortName	恒信40A1
0	25067	2021-08-12	NULL	2030	SecurityExchangeCode	189424
0	25067	2021-08-12	NULL	2036	InterestDays	365
0	25067	2021-08-12	NULL	2037	InterestRateCalculation	按天
0	25067	2021-08-12	NULL	13024	ClassType	FirstClass
0	25067	2021-08-12	NULL	13153	InterestUnpaidDispose	1
0	25067	2021-08-12	NULL	13190	InterestPaymentType	BaseOnPaymentDay
0	25067	2021-08-12	NULL	13192	IsChinabondDelimit	1
0	25067	2021-08-12	NULL	13202	InterestRoundingRule	0
0	25067	2021-08-12	NULL	13350	TotalInterestRoundingRule	0
0	25067	2021-08-12	NULL	13351	PrincipalRoundingRule	0
0	25067	2021-08-12	NULL	13352	TotalPrincipalRoundingRule	0
1	25067	2021-08-12	NULL	2000	ClassName	AAA
1	25067	2021-08-12	NULL	2001	IssueDate	2021-06-17
1	25067	2021-08-12	NULL	2002	CurrencyOfIssuance	CNY
1	25067	2021-08-12	NULL	2003	OfferAmount	370000000
1	25067	2021-08-12	NULL	2009	CouponBasis	3.8
1	25067	2021-08-12	NULL	2011	CouponPaymentReference	固定利率
1	25067	2021-08-12	NULL	2014	Denomination	100
1	25067	2021-08-12	NULL	2015	LegalMaturityDate	2023-06-26
1	25067	2021-08-12	NULL	2017	OriginalCreditRating	AAA
1	25067	2021-08-12	NULL	2019	PaymentConvention	过手摊还
1	25067	2021-08-12	NULL	2029	ShortName	恒信40A2
1	25067	2021-08-12	NULL	2030	SecurityExchangeCode	189425
1	25067	2021-08-12	NULL	2036	InterestDays	365
1	25067	2021-08-12	NULL	2037	InterestRateCalculation	按天
1	25067	2021-08-12	NULL	13024	ClassType	FirstClass
1	25067	2021-08-12	NULL	13153	InterestUnpaidDispose	1
1	25067	2021-08-12	NULL	13190	InterestPaymentType	BaseOnPaymentDay
1	25067	2021-08-12	NULL	13192	IsChinabondDelimit	1
1	25067	2021-08-12	NULL	13202	InterestRoundingRule	0
1	25067	2021-08-12	NULL	13350	TotalInterestRoundingRule	0
1	25067	2021-08-12	NULL	13351	PrincipalRoundingRule	0
1	25067	2021-08-12	NULL	13352	TotalPrincipalRoundingRule	0
2	25067	2021-08-12	NULL	2000	ClassName	AAA
2	25067	2021-08-12	NULL	2001	IssueDate	2021-06-17
2	25067	2021-08-12	NULL	2002	CurrencyOfIssuance	CNY
2	25067	2021-08-12	NULL	2003	OfferAmount	50000000
2	25067	2021-08-12	NULL	2009	CouponBasis	4.4
2	25067	2021-08-12	NULL	2011	CouponPaymentReference	固定利率
2	25067	2021-08-12	NULL	2014	Denomination	100
2	25067	2021-08-12	NULL	2015	LegalMaturityDate	2023-08-26
2	25067	2021-08-12	NULL	2017	OriginalCreditRating	AAA
2	25067	2021-08-12	NULL	2019	PaymentConvention	过手摊还
2	25067	2021-08-12	NULL	2029	ShortName	恒信40A3
2	25067	2021-08-12	NULL	2030	SecurityExchangeCode	189426
2	25067	2021-08-12	NULL	2036	InterestDays	365
2	25067	2021-08-12	NULL	2037	InterestRateCalculation	按天
2	25067	2021-08-12	NULL	13024	ClassType	FirstClass
2	25067	2021-08-12	NULL	13153	InterestUnpaidDispose	1
2	25067	2021-08-12	NULL	13190	InterestPaymentType	BaseOnPaymentDay
2	25067	2021-08-12	NULL	13192	IsChinabondDelimit	1
2	25067	2021-08-12	NULL	13202	InterestRoundingRule	0
2	25067	2021-08-12	NULL	13350	TotalInterestRoundingRule	0
3	25067	2021-08-12	NULL	2000	ClassName	NR
3	25067	2021-08-12	NULL	2001	IssueDate	2021-06-17
3	25067	2021-08-12	NULL	2002	CurrencyOfIssuance	CNY
3	25067	2021-08-12	NULL	2003	OfferAmount	50000000
3	25067	2021-08-12	NULL	2009	CouponBasis	0
3	25067	2021-08-12	NULL	2011	CouponPaymentReference	固定利率
3	25067	2021-08-12	NULL	2014	Denomination	100
3	25067	2021-08-12	NULL	2015	LegalMaturityDate	2024-05-26
3	25067	2021-08-12	NULL	2017	OriginalCreditRating	NR
3	25067	2021-08-12	NULL	2019	PaymentConvention	过手摊还
3	25067	2021-08-12	NULL	2029	ShortName	恒信40次
3	25067	2021-08-12	NULL	2030	SecurityExchangeCode	189427
3	25067	2021-08-12	NULL	2036	InterestDays	365
3	25067	2021-08-12	NULL	2037	InterestRateCalculation	按天
3	25067	2021-08-12	NULL	13024	ClassType	EquityClass
3	25067	2021-08-12	NULL	13025	MultipleRatings	[{"RatingDate":"","Rating":"","IsPlus":false}]
3	25067	2021-08-12	NULL	13153	InterestUnpaidDispose	1
3	25067	2021-08-12	NULL	13190	InterestPaymentType	BaseOnPaymentDay
3	25067	2021-08-12	NULL	13192	IsChinabondDelimit	1
3	25067	2021-08-12	NULL	13202	InterestRoundingRule	0
3	25067	2021-08-12	NULL	13350	TotalInterestRoundingRule	0
3	25067	2021-08-12	NULL	13351	PrincipalRoundingRule	0
3	25067	2021-08-12	NULL	13352	TotalPrincipalRoundingRule	0
3	25067	2021-08-12	NULL	13356	PrinciplePlanIsEffective	0
3	25067	2021-08-12	NULL	13357	InterestPlanIsEffective	0
2	25067	2021-08-12	NULL	13351	PrincipalRoundingRule	0
2	25067	2021-08-12	NULL	13352	TotalPrincipalRoundingRule	0

select distinct ItemId, ItemCode
from [TrustManagement].[TrustBond];

ItemId	ItemCode
2003	OfferAmount
2011	CouponPaymentReference
2015	LegalMaturityDate
2019	PaymentConvention
2032	PaymentFrequence
2033	RatingAgent
2034	PrincipalPayStartDate
2036	InterestDays
13024	ClassType
13025	MultipleRatings
13153	InterestUnpaidDispose
13183	InterestPaymentType
13184	InterestRoundingRule
13190	InterestPaymentType
13202	InterestRoundingRule
13204	PrincipalRoundingRule
13274	InterestPaymentType
13352	TotalPrincipalRoundingRule
13356	PrinciplePlanIsEffective
13377	BaseRate
13378	CurrentRate
14397	CurrentCouponBasis
16008	RequiredDefaultRate
16009	StressScenario1Amount
16010	StressScenario2Amount
2000	ClassName
2001	IssueDate
2002	CurrencyOfIssuance
2009	CouponBasis
2014	Denomination
2017	OriginalCreditRating
2029	ShortName
2030	SecurityExchangeCode
2031	PrincipalSchedule
2035	PrincipalPayTerm
2037	InterestRateCalculation
2038	Islisting_EquityClass
13185	IsChinabondDelimit
13192	IsChinabondDelimit
13203	TotalInterestRoundingRule
13205	TotalPrincipalRoundingRule
13346	BookKeepingDate
13350	TotalInterestRoundingRule
13351	PrincipalRoundingRule
13357	InterestPlanIsEffective
14396	CurrentOfferAmount
16011	StressBP

    """
    print("\n" + "=" * 80)
    print("导入数据到TrustManagement.TrustBond")
    print("=" * 80)
    
    # ==================== Excel字段 -> ItemCode 映射 ====================
    # Excel列名 -> (ItemId, ItemCode, 值转换函数)
    FIELD_MAPPING = {
        # 证券名称 -> ShortName (2029)
        "证券名称": (2029, "ShortName", lambda x: str(x) if pd.notna(x) else ""),
        
        # 证券代码 -> SecurityExchangeCode (2030)
        # 去掉后缀(.IB, .SZ, .SH等)
        "证券代码": (2030, "SecurityExchangeCode", lambda x: str(x).split('.')[0] if pd.notna(x) else ""),
        
        # 发行量 -> OfferAmount (2003)
        "发行量": (2003, "OfferAmount", lambda x: str(int(x)) if pd.notna(x) and x else "0"),
        
        # 还本方式 -> PaymentConvention (2019)
        # 到期还本 -> 到期一次性还本, 过手 -> 过手摊还
        "还本方式": (2019, "PaymentConvention", lambda x: "到期一次性还本" if str(x) == "到期还本" else ("过手摊还" if str(x) == "过手" else str(x)) if pd.notna(x) else ""),
        
        # 类型 -> ClassType (13024)
        # 优先级 -> FirstClass, 次级 -> EquityClass, 夹层级 -> SubClass
        "类型": (13024, "ClassType", lambda x: "FirstClass" if "优先" in str(x) else ("EquityClass" if "次" in str(x) else ("SubClass" if "夹层" in str(x) else str(x)))),
        
        # 发行利率 -> CouponBasis (2009)
        # Excel中是小数形式(0.0215)，需要乘100转为百分比(2.15)
        "发行利率": (2009, "CouponBasis", lambda x: str(round(float(x) * 100, 4)) if pd.notna(x) and x else "0"),
        
        # 当前利率 -> CurrentCouponBasis (14397)
        "当前利率": (14397, "CurrentCouponBasis", lambda x: str(round(float(x) * 100, 4)) if pd.notna(x) and x else "0"),
        
        # 预计到期日 -> LegalMaturityDate (2015)
        "预计到期日": (2015, "LegalMaturityDate", lambda x: str(x)[:10] if pd.notna(x) else ""),
        
        # 利率形式 -> CouponPaymentReference (2011)
        # 固定 -> 固定利率, 浮动 -> 浮动利率, 空值 -> 固定利率
        "利率形式": (2011, "CouponPaymentReference", lambda x: "固定利率" if not pd.notna(x) or not str(x).strip() or str(x) == "固定" else ("浮动利率" if str(x) == "浮动" else "固定利率")),
    }
    
    # 动态评级字段映射（原始评级）
    # xxx（原始）-> OriginalCreditRating (2017)
    # xxx（当前）-> ClassName (2000) 作为当前评级
    
    # 默认值字段（每个证券都需要插入的固定值）
    DEFAULT_FIELDS = [
        (2002, "CurrencyOfIssuance", "CNY"),           # 货币
        (2014, "Denomination", "100"),                  # 面值
        (2036, "InterestDays", "365"),                  # 计息天数
        (2037, "InterestRateCalculation", "按天"),      # 利率计算方式
        (13153, "InterestUnpaidDispose", "1"),          # 利息未付处理
        (13190, "InterestPaymentType", "BaseOnPaymentDay"),  # 付息类型
        (13192, "IsChinabondDelimit", "1"),             # 是否中债划界
        (13202, "InterestRoundingRule", "0"),           # 利息舍入规则
        (13350, "TotalInterestRoundingRule", "0"),      # 总利息舍入规则
        (13351, "PrincipalRoundingRule", "0"),          # 本金舍入规则
        (13352, "TotalPrincipalRoundingRule", "0"),     # 总本金舍入规则
    ]
    # ================================================================
    
    # 创建产品名称到TrustId的映射
    name_to_id = {name: trust_id for name, trust_id in matched}
    
    cursor = conn.cursor()
    
    insert_count = 0
    bond_count = 0
    sql_statements = []  # 存储所有SQL语句
    
    # 按产品分组处理
    for product_name in df["产品全称"].unique():
        if product_name not in name_to_id:
            continue
        
        trust_id = name_to_id[product_name]
        product_df = df[df["产品全称"] == product_name]
        
        # 查询该产品已有的证券数量（用于日志）
        cursor.execute(
            f"SELECT COUNT(DISTINCT TrustBondId) FROM TrustManagement.TrustBond WHERE TrustId = {trust_id}"
        )
        existing_count = cursor.fetchone()[0]
        
        print(f"\n处理产品: {product_name} (TrustId={trust_id})")
        print(f"  数据库已有证券数: {existing_count}，将先删除再重新插入")
        
        # 添加产品注释到SQL脚本
        sql_statements.append(f"-- ============================================================")
        sql_statements.append(f"-- 产品: {product_name}")
        sql_statements.append(f"-- TrustId: {trust_id}")
        sql_statements.append(f"-- 原有证券数: {existing_count}，新证券数: {len(product_df)}")
        sql_statements.append(f"-- ============================================================")
        
        # 先删除该产品的所有TrustBond数据
        sql_statements.append(f"-- 删除原有数据")
        sql_statements.append(f"DELETE FROM TrustManagement.TrustBond WHERE TrustId = {trust_id};")
        sql_statements.append("")
        
        # 按证券代码排序（去掉后缀后按数字排序）
        # 例如: 082400476.IB -> 82400476, 135572.SZ -> 135572
        def get_sort_key(code):
            if pd.isna(code) or not code:
                return 0
            # 去掉后缀(.IB, .SZ等)
            code_clean = str(code).split('.')[0]
            # 去掉前导零，转为整数
            try:
                return int(code_clean.lstrip('0') or '0')
            except:
                return 0
        
        product_df = product_df.copy()
        product_df['_sort_key'] = product_df['证券代码'].apply(get_sort_key)
        product_df = product_df.sort_values('_sort_key')
        
        print(f"  证券排序: {list(product_df['证券代码'].values)}")
        
        # 收集该产品所有证券的VALUES
        all_values_list = []
        bond_comments = []  # 证券注释
        start_date = datetime.now().strftime("%Y-%m-%d")
        
        # 遍历该产品的每个证券，TrustBondId从0开始
        for bond_idx, (_, row) in enumerate(product_df.iterrows()):
            bond_code = row.get("证券代码", "")
            bond_name = row.get("证券名称", "")
            
            trust_bond_id = bond_idx  # TrustBondId从0开始
            
            print(f"  插入证券 TrustBondId={trust_bond_id}: {bond_name} ({bond_code})")
            bond_comments.append(f"--   {trust_bond_id}: {bond_name} ({bond_code})")
            
            # 获取评级字段（动态列名，最多只有一家机构有值）
            original_rating = ""
            current_rating = ""
            for col in row.index:
                if "原始" in str(col) and "利率" not in str(col):
                    val = row[col] if pd.notna(row[col]) else ""
                    if val and not original_rating:  # 找到第一个非空值
                        original_rating = val
                elif "当前" in str(col) and "利率" not in str(col):
                    val = row[col] if pd.notna(row[col]) else ""
                    if val and not current_rating:  # 找到第一个非空值
                        current_rating = val
            
            # 准备插入的字段列表
            fields_to_insert = []
            
            # 1. 添加映射字段
            for excel_col, (item_id, item_code, converter) in FIELD_MAPPING.items():
                if excel_col in row.index:
                    value = converter(row[excel_col])
                    if value:  # 只插入非空值
                        fields_to_insert.append((item_id, item_code, value))
            
            # 2. 添加评级字段
            if original_rating:
                fields_to_insert.append((2017, "OriginalCreditRating", str(original_rating)))
            if current_rating:
                fields_to_insert.append((2000, "ClassName", str(current_rating)))
            
            # 3. 添加IssueDate（使用产品的TrustStartDate）
            trust_start_date = db_start_dates.get(trust_id, "")
            if trust_start_date:
                # 只取日期部分（前10个字符，格式：YYYY-MM-DD）
                issue_date = str(trust_start_date)[:10]
                fields_to_insert.append((2001, "IssueDate", issue_date))
            
            # 4. 添加默认字段
            for item_id, item_code, value in DEFAULT_FIELDS:
                fields_to_insert.append((item_id, item_code, value))
            
            # 构建该证券的VALUES
            for item_id, item_code, item_value in fields_to_insert:
                # 转义单引号
                item_value_escaped = str(item_value).replace("'", "''")
                all_values_list.append(
                    f"({trust_bond_id}, {trust_id}, '{start_date}', NULL, {item_id}, '{item_code}', N'{item_value_escaped}')"
                )
            
            insert_count += len(fields_to_insert)
            bond_count += 1
            print(f"    ✓ 生成 {len(fields_to_insert)} 条属性记录")
        
        # 为该产品生成一条合并的INSERT语句
        sql_statements.append("-- 证券列表:")
        sql_statements.extend(bond_comments)
        sql_statements.append(f"INSERT INTO TrustManagement.TrustBond (TrustBondId, TrustId, StartDate, EndDate, ItemId, ItemCode, ItemValue)")
        sql_statements.append(f"VALUES")
        # 每个VALUE元组单独一行，便于查看
        for i, val in enumerate(all_values_list):
            if i < len(all_values_list) - 1:
                sql_statements.append(f"    {val},")
            else:
                sql_statements.append(f"    {val};")
        sql_statements.append("")  # 空行分隔
    
    # 生成SQL脚本文件
    sql_file = "cnabs_import_bonds.sql"
    
    print("\n" + "=" * 80)
    print(f"生成SQL脚本: {sql_file}")
    print("=" * 80)
    
    with open(sql_file, "w", encoding="utf-8") as f:
        # 写入文件头
        f.write("-- ============================================================\n")
        f.write("-- CNABS证券数据导入脚本\n")
        f.write(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- 数据来源: {DEFAULT_EXCEL}\n")
        f.write(f"-- 涉及产品数: {len(matched)} 个\n")
        f.write(f"-- 证券数: {bond_count} 个\n")
        f.write(f"-- 属性记录数: {insert_count} 条\n")
        f.write("-- ============================================================\n\n")
        
        # 写入USE语句
        f.write("USE PortfolioManagement;\nGO\n\n")
        
        # 写入事务开始
        f.write("BEGIN TRANSACTION;\n\n")
        
        # 写入所有SQL语句
        for stmt in sql_statements:
            f.write(stmt + "\n")
        
        # 写入事务提交（注释掉，让DBA确认后手动执行）
        f.write("\n-- 确认数据无误后，执行以下语句提交事务：\n")
        f.write("-- COMMIT TRANSACTION;\n\n")
        f.write("-- 如果需要回滚，执行：\n")
        f.write("-- ROLLBACK TRANSACTION;\n")
    
    print(f"✓ SQL脚本已保存到: {sql_file}")
    
    # 打印统计
    print("\n" + "=" * 80)
    print("生成统计")
    print("=" * 80)
    print(f"  涉及产品数: {len(matched)} 个")
    print(f"  证券数: {bond_count} 个")
    print(f"  属性记录数: {insert_count} 条")
    print(f"\n请将 {sql_file} 发送给DBA检查并执行")


def main():
    """主函数"""
    print("=" * 80)
    print("cnabs_securities.xlsx 导入工具")
    print("=" * 80)
    
    # 解析命令行参数
    check_only = False
    excel_file = DEFAULT_EXCEL
    
    for arg in sys.argv[1:]:
        if arg == "--check-only":
            check_only = True
        elif arg.endswith(".xlsx"):
            excel_file = arg
    
    print(f"Excel文件: {excel_file}")
    print(f"模式: {'仅检查' if check_only else '检查并导入'}")
    print("=" * 80)
    
    # 1. 读取Excel
    df = read_excel(excel_file)
    if df is None:
        return
    
    # 检查必要的列
    if "产品全称" not in df.columns:
        print("✗ Excel缺少'产品全称'列（A列）")
        return
    
    # 2. 连接数据库
    conn = get_sql_connection()
    if conn is None:
        return
    
    try:
        # 3. 获取Trust映射
        trust_mapping = get_trust_mapping(conn)
        if not trust_mapping:
            print("✗ 无法获取Trust映射，退出")
            return
        
        # 4. 检查产品匹配
        matched, unmatched, missing_start_date, db_start_dates = check_product_matching(df, trust_mapping, conn)
        
        if not matched:
            print("\n✗ 没有匹配的产品，无法导入")
            return
        
        # 检查是否有产品缺少设立日
        if missing_start_date:
            print(f"\n✗ 有 {len(missing_start_date)} 个产品缺少设立日，无法继续导入")
            return
        
        if unmatched:
            print(f"\n⚠️ 警告: 有 {len(unmatched)} 个产品未匹配")
            if not check_only:
                response = input("是否继续导入已匹配的产品？(y/n): ").strip().lower()
                if response != 'y':
                    print("取消导入")
                    return
        
        # 5. 导入数据（如果不是仅检查模式）
        if not check_only:
            import_bonds(conn, df, matched, db_start_dates)
        else:
            print("\n[仅检查模式] 跳过导入步骤")
    
    finally:
        conn.close()
        print("\n数据库连接已关闭")


if __name__ == "__main__":
    main()
