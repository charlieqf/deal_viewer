# -*- coding: utf-8 -*-
"""
检查数据库中是否存在缺失的产品
请在服务器上运行此脚本：python check_missing_products.py
"""
import pyodbc

def get_sql_connection():
    try:
        conn_str = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=113.125.202.171,52482;"
            "Database=PortfolioManagement;"
            "UID=sa;"
            "PWD=PasswordGS2017;"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        print("Database error:", e)
        return None

def check_products():
    conn = get_sql_connection()
    if not conn:
        print("无法连接数据库")
        return
    
    cursor = conn.cursor()
    
    # 需要检查的产品名称 (从日志中提取的)
    products_to_check = [
        "广州市自来水有限公司2025年度2号第一期绿色资产支持票据",
        "中联重科股份有限公司2025年度第一期保供稳链资产支持票据(科创票据)",
        "中联重科股份有限公司2023年度第二期保供稳链资产支持票据(科创票据)",
        "中联重科股份有限公司2024年度第一期保供稳链资产支持票据(乡村振兴科创票据)",
        "广州越秀融资租赁有限公司2024年度第一期资产支持票据",
    ]
    
    print("=" * 80)
    print("检查缺失产品")
    print("=" * 80)
    
    for product_name in products_to_check:
        print(f"\n【检查】{product_name}")
        
        # 1. 精确匹配
        sql = f"SELECT TrustId, TrustCode, TrustName FROM TrustManagement.Trust WHERE TrustName = N'{product_name}'"
        cursor.execute(sql)
        rows = cursor.fetchall()
        
        if rows:
            print("  ✓ 找到精确匹配:")
            for row in rows:
                print(f"    TrustId={row[0]}, TrustCode={row[1]}, TrustName={row[2]}")
        else:
            print("  ✗ 未找到精确匹配")
            
            # 2. 模糊匹配 - 使用关键词
            keywords = []
            if "广州市自来水" in product_name:
                keywords = ["广州市自来水", "2025", "资产支持票据"]
            elif "中联重科" in product_name:
                if "2025" in product_name and "保供稳链" in product_name and "科创票据" in product_name and "乡村" not in product_name:
                    keywords = ["中联重科", "2025", "第一期", "保供稳链", "科创"]
                elif "2023" in product_name:
                    keywords = ["中联重科", "2023", "第二期", "保供稳链"]
                elif "乡村振兴" in product_name:
                    keywords = ["中联重科", "2024", "乡村振兴"]
            elif "广州越秀融资租赁" in product_name:
                keywords = ["广州越秀融资租赁", "2024", "第一期"]
            
            if keywords:
                like_clause = " AND ".join([f"TrustName LIKE N'%{kw}%'" for kw in keywords])
                sql = f"SELECT TOP 10 TrustId, TrustCode, TrustName FROM TrustManagement.Trust WHERE {like_clause}"
                print(f"  尝试模糊匹配: {keywords}")
                try:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    if rows:
                        print("  ▸ 找到相似产品:")
                        for row in rows:
                            print(f"    TrustId={row[0]}, TrustCode={row[1]}")
                            print(f"      TrustName={row[2]}")
                    else:
                        print("  ▸ 未找到相似产品")
                except Exception as e:
                    print(f"  ▸ 查询出错: {e}")
            
            # 3. 查找该公司的所有产品
            company_name = product_name.split("20")[0]
            if company_name:
                sql = f"SELECT TOP 20 TrustId, TrustCode, TrustName FROM TrustManagement.Trust WHERE TrustName LIKE N'{company_name}%' ORDER BY TrustId DESC"
                print(f"\n  查找公司 [{company_name}] 的所有产品:")
                try:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    if rows:
                        for row in rows:
                            print(f"    TrustId={row[0]}, TrustCode={row[1]}")
                            print(f"      TrustName={row[2]}")
                    else:
                        print("    未找到该公司的任何产品")
                except Exception as e:
                    print(f"    查询出错: {e}")
    
    conn.close()
    print("\n" + "=" * 80)
    print("检查完成")
    print("=" * 80)

if __name__ == "__main__":
    check_products()
