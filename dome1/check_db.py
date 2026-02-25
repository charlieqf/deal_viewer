import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=113.125.202.171,52482;"
    "DATABASE=PortfolioManagement;"
    "UID=sa;"
    "PWD=PasswordGS2017;"
    "Encrypt=no;"
    "TrustServerCertificate=yes"
)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    print("More HuaYu products:")
    cursor.execute("select top 30 TrustCode, TrustName from TrustManagement.Trust where TrustName like N'%华驭%' order by TrustId desc")
    for r in cursor.fetchall():
        print(f"{r.TrustCode} | {r.TrustName}")

    print("\nRecent products from 2024/2025:")
    cursor.execute("select top 10 TrustCode, TrustName from TrustManagement.Trust where TrustCode like '%2024%' or TrustCode like '%2025%' order by TrustId desc")
    for r in cursor.fetchall():
        print(f"{r.TrustCode} | {r.TrustName}")

    conn.close()
except Exception as e:
    print(f"Error: {e}")
