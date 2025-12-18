
import pyodbc

def get_sql_connection():
    try:
        # Establish connection using pyodbc
        conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
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

conn = get_sql_connection()
cursor = conn.cursor()

trust_id = 34358
sql = f"select TrustDocumentID, FileName, FileType, FileCategory from PortfolioManagement.DV.TrustAssociatedDocument where TrustId={trust_id}"
print(f"Executing: {sql}")
cursor.execute(sql)
rows = cursor.fetchall()

if not rows:
    print("No documents found for TrustId", trust_id)
else:
    print(f"Found {len(rows)} documents:")
    for row in rows:
        print(f"ID: {row[0]}, Name: {row[1]}, Type: {row[2]}, Category: {row[3]}")

conn.close()
