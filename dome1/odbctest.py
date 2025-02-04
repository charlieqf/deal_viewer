import pyodbc

def check_trust_code(trust_code, product_name, cmProd):
    try:
        # Establish connection using pyodbc
        conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=113.125.202.171,52482;"
            "Database=PortfolioManagement;"  # Use your database name
            "UID=sa;"           # Use your username
            "PWD=PasswordGS2017;"           # Use your password
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Use parameterized query
        sql_check = "SELECT 1 FROM TrustManagement.Trust WHERE Trustcode = ?"
        cursor.execute(sql_check, trust_code)

        # Fetch result
        if cursor.fetchone():
            print('code重名', trust_code)
            cmProd.append(f"{product_name}:{trust_code}")
            return
    except pyodbc.Error as e:
        print("Database error:", e)
    finally:
        # Close the connection
        conn.close()

# Example usage
cmProd = []
check_trust_code('some_trust_code', 'some_product_name', cmProd)
print(cmProd)
