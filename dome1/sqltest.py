import pymssql

def test_connection():
    try:
        # Establish connection with pymssql
        conn = pymssql.connect(
            server='113.125.202.171',
            port=52482,
            user='sa',
            password='PasswordGS2017',
            database='PortfolioManagement',
            charset='utf8',
            as_dict=True  # Optional, returns results as dictionaries
        )
        print("Connection successful")
        conn.close()
    except pymssql.Error as e:
        print("Database connection error:", e)

test_connection()
