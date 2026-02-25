import pyodbc
import sys

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=113.125.202.171,52482;"
    "DATABASE=PortfolioManagement;"
    "UID=sa;"
    "PWD=PasswordGS2017;"
    "Encrypt=no;"
    "TrustServerCertificate=yes"
)

def run_fix():
    print("Connecting to DB...")
    # Use a longer connection timeout
    conn = pyodbc.connect(conn_str, timeout=60)
    cursor = conn.cursor()
    
    # Set query timeout to be very aggressive if needed, or long enough
    cursor.execute("SET LOCK_TIMEOUT 10000") # 10 seconds wait for locks
    
    try:
        print("1. Updating TrustManagement.Trust...")
        cursor.execute("""
            UPDATE TrustManagement.Trust 
            SET TrustCode = 'HuaYu_AUTO2025-17', 
                TrustNameShort = N'华驭-17' 
            WHERE TrustId = 34391
        """)
        conn.commit()
        print("Success: Trust table updated.")

        print("2. Updating FixedIncomeSuite.Analysis.Trust...")
        cursor.execute("SELECT 1 FROM FixedIncomeSuite.Analysis.Trust WHERE TrustId = 34391")
        if not cursor.fetchone():
            cursor.execute("SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust ON")
            cursor.execute("""
                INSERT INTO FixedIncomeSuite.Analysis.Trust(TrustId, TrustCode, TrustName) 
                VALUES (34391, 'HuaYu_AUTO2025-17', N'华驭第十七期汽车抵押贷款支持证券')
            """)
            cursor.execute("SET IDENTITY_INSERT FixedIncomeSuite.Analysis.Trust OFF")
            conn.commit()
            print("Success: Analysis.Trust updated.")
        else:
            print("Skip: Analysis.Trust already has record.")

        print("3. Updating TrustManagement.TrustInfoExtension...")
        cursor.execute("SELECT 1 FROM TrustManagement.TrustInfoExtension WHERE TrustId = 34391")
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO TrustManagement.TrustInfoExtension(TrustId, StartDate, EndDate, ItemId, ItemCode, ItemValue) 
                VALUES 
                (34391, GETDATE(), null, null, 'MarketCategory','CAS'),
                (34391, GETDATE(), null, null, 'RegulatoryOrg','CBIRC'),
                (34391, GETDATE(), null, null, 'MarketPlace', 'InterBank'),
                (34391, GETDATE(), null, null, 'AssetType','CarLoan'),
                (34391, GETDATE(), null, null, 'BasicAssetType','Others'),
                (34391, GETDATE(), null, null, 'CollectionMethod', 'PublicOffering')
            """)
            conn.commit()
            print("Success: TrustInfoExtension updated.")
        else:
            print("Skip: TrustInfoExtension already has record.")

        print("4. Updating dbo.ReportMaxNper...")
        cursor.execute("SELECT 1 FROM dbo.ReportMaxNper WHERE TrustId = 34391")
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO dbo.ReportMaxNper (TrustId, TrustNameShort, MaxNper, Status)
                VALUES (34391, N'华驭-17', 0, N'存续期')
            """)
            conn.commit()
            print("Success: ReportMaxNper updated.")
        else:
            print("Skip: ReportMaxNper already has record.")

        print("\nFinal Verification:")
        cursor.execute("SELECT TrustCode, TrustNameShort FROM TrustManagement.Trust WHERE TrustId = 34391")
        row = cursor.fetchone()
        if row:
            print(f"Code: {row[0]}")
            print(f"ShortName: {row[1]}")
            if row[0] == 'HuaYu_AUTO2025-17':
                print("FIX VERIFIED.")
            else:
                print("FIX FAILED TO APPLY (Code mismatch).")
        else:
            print("Record not found.")

    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    run_fix()
