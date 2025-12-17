#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cnabs_import_dates.py - 从cnabs_securities.xlsx的"产品信息"sheet导入起息日到TrustManagement.TrustExtension

功能：
1. 读取cnabs_securities.xlsx的"产品信息"sheet（两列：产品全称、起息日）
2. 检查Excel中产品全称与TrustManagement.Trust表中TrustName的匹配
3. 检查数据库中是否已存在TrustStartDate，如果已存在则跳过
4. 生成SQL脚本插入TrustStartDate到TrustManagement.TrustExtension

使用方法：
    python cnabs_import_dates.py                    # 默认读取cnabs_securities.xlsx
    python cnabs_import_dates.py your_file.xlsx    # 指定Excel文件
    python cnabs_import_dates.py --check-only      # 仅检查匹配，不导入
"""

import os
import sys
import pandas as pd
import pyodbc
from datetime import datetime


# ==================== 配置 ====================
DEFAULT_EXCEL = "cnabs_securities.xlsx"

# Excel "产品信息" sheet字段
'''
产品全称	起息日
深圳能源集团股份有限公司2024年度新能源1号第一期绿色定向资产支持商业票据	2024-04-29
京东科技荟臻2号第1期资产支持专项计划	2022-09-30
君创国际融资租赁有限公司2023年度第二期普惠定向资产支持票据(债券通)	2023-08-09
'''

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
    """读取Excel文件的'产品信息'sheet"""
    if not os.path.exists(file_path):
        print(f"✗ 文件不存在: {file_path}")
        return None
    
    try:
        df = pd.read_excel(file_path, sheet_name='产品信息', engine='openpyxl')
        print(f"✓ 读取Excel成功: {file_path} (sheet: 产品信息)")
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
    检查Excel中产品全称与数据库TrustName的匹配，以及是否已有TrustStartDate
    
    返回:
        to_import: [(产品全称, TrustId, 起息日), ...]  需要导入的（数据库中无TrustStartDate）
        already_exists: [(产品全称, TrustId, 起息日, 数据库中的日期), ...]  已存在的
        unmatched: [产品全称, ...]  未匹配的
    """
    print("\n" + "=" * 80)
    print("产品匹配检查")
    print("=" * 80)
    
    # 获取Excel中所有产品
    print(f"Excel中产品数: {len(df)}")
    
    matched = []
    unmatched = []
    
    for _, row in df.iterrows():
        product_name = row["产品全称"]
        issue_date = row.get("起息日", "")
        
        if product_name in trust_mapping:
            matched.append((product_name, trust_mapping[product_name], issue_date))
        else:
            unmatched.append(product_name)
    
    # 查询数据库中已有的TrustStartDate
    db_start_dates = {}  # TrustId -> TrustStartDate
    if matched:
        try:
            cursor = conn.cursor()
            trust_ids_str = ",".join([str(m[1]) for m in matched])
            
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
    
    # 分类：需要导入的 vs 已存在的
    to_import = []
    already_exists = []
    
    for product_name, trust_id, issue_date in matched:
        db_date = db_start_dates.get(trust_id)
        if db_date:
            already_exists.append((product_name, trust_id, issue_date, db_date))
        else:
            to_import.append((product_name, trust_id, issue_date))
    
    # 打印匹配结果
    print(f"\n匹配成功: {len(matched)} 个产品")
    print(f"未匹配: {len(unmatched)} 个产品")
    print(f"需要导入: {len(to_import)} 个产品")
    print(f"已有设立日(跳过): {len(already_exists)} 个产品")
    
    if unmatched:
        print("\n--- 未匹配的产品列表 ---")
        for i, name in enumerate(unmatched, 1):
            print(f"  {i}. {name}")
    
    if already_exists:
        print("\n--- 已有设立日的产品(跳过) ---")
        mismatch_count = 0
        for i, (name, trust_id, excel_date, db_date) in enumerate(already_exists, 1):
            excel_date_str = str(excel_date)[:10] if pd.notna(excel_date) else "无"
            db_date_str = str(db_date)[:10]
            # 检查日期是否一致
            if excel_date_str != db_date_str:
                print(f"  {i}. {name} -> TrustId={trust_id} (Excel: {excel_date_str}, DB: {db_date_str}) ⚠️ 日期不一致")
                mismatch_count += 1
            else:
                print(f"  {i}. {name} -> TrustId={trust_id} (Excel: {excel_date_str}, DB: {db_date_str})")
        
        if mismatch_count > 0:
            print(f"\n⚠️ 警告: {mismatch_count} 个产品的Excel起息日与数据库TrustStartDate不一致")
    
    if to_import:
        print("\n--- 需要导入的产品 ---")
        for i, (name, trust_id, issue_date) in enumerate(to_import, 1):
            date_str = str(issue_date)[:10] if pd.notna(issue_date) else "无"
            print(f"  {i}. {name} -> TrustId={trust_id} (起息日: {date_str})")
    
    return to_import, already_exists, unmatched

def import_start_dates(to_import):
    """
    生成SQL脚本，将起息日导入到TrustManagement.TrustExtension表
    
    TrustExtension表结构：
        TrustId, StartDate, EndDate, ItemId, ItemCode, ItemValue
        
    TrustStartDate的ItemId = 1001, ItemCode = 'TrustStartDate'
    """
    print("\n" + "=" * 80)
    print("生成TrustStartDate导入SQL脚本")
    print("=" * 80)
    
    if not to_import:
        print("没有需要导入的数据")
        return
    
    sql_statements = []
    start_date = datetime.now().strftime("%Y-%m-%d")
    
    # 为每个产品生成INSERT语句
    for product_name, trust_id, issue_date in to_import:
        if not pd.notna(issue_date):
            print(f"  跳过: {product_name} (TrustId={trust_id}) - 起息日为空")
            continue
        
        # 格式化日期（只取前10个字符）
        date_str = str(issue_date)[:10]
        
        sql_statements.append(f"-- {product_name}")
        sql_statements.append(
            f"INSERT INTO TrustManagement.TrustExtension (TrustId, StartDate, EndDate, ItemId, ItemCode, ItemValue) "
            f"VALUES ({trust_id}, '{start_date}', NULL, 1001, 'TrustStartDate', N'{date_str}');"
        )
        sql_statements.append("")
        
        print(f"  ✓ {product_name} -> TrustId={trust_id}, TrustStartDate={date_str}")
    
    # 生成SQL脚本文件
    sql_file = "cnabs_import_dates.sql"
    
    print("\n" + "=" * 80)
    print(f"生成SQL脚本: {sql_file}")
    print("=" * 80)
    
    with open(sql_file, "w", encoding="utf-8") as f:
        # 写入文件头
        f.write("-- ============================================================\n")
        f.write("-- CNABS起息日(TrustStartDate)导入脚本\n")
        f.write(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- 数据来源: {DEFAULT_EXCEL} (产品信息 sheet)\n")
        f.write(f"-- 导入产品数: {len(to_import)} 个\n")
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
    print(f"  导入产品数: {len(to_import)} 个")
    print(f"\n请将 {sql_file} 发送给DBA检查并执行")


def main():
    """主函数"""
    print("=" * 80)
    print("cnabs_securities.xlsx 起息日导入工具")
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
    print(f"Sheet: 产品信息")
    print(f"模式: {'仅检查' if check_only else '检查并导入'}")
    print("=" * 80)
    
    # 1. 读取Excel的"产品信息"sheet
    df = read_excel(excel_file)
    if df is None:
        return
    
    # 检查必要的列
    if "产品全称" not in df.columns:
        print("✗ Excel缺少'产品全称'列")
        return
    if "起息日" not in df.columns:
        print("✗ Excel缺少'起息日'列")
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
        to_import, already_exists, unmatched = check_product_matching(df, trust_mapping, conn)
        
        if not to_import:
            print("\n✓ 没有需要导入的数据（所有匹配产品已有设立日）")
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
            import_start_dates(to_import)
        else:
            print("\n[仅检查模式] 跳过导入步骤")
    
    finally:
        conn.close()
        print("\n数据库连接已关闭")


if __name__ == "__main__":
    main()
