# -*- coding: utf-8 -*-
import pandas as pd

import os
import sys

# Database connection settings
DB_SERVER = "192.168.1.149\\mssql"
# DB_PORT = 1433 # Not needed for ODBC named instance
DB_NAME = "PortfolioManagement"
DB_USER = "sa"
DB_PASSWORD = "PasswordGS15"

import pyodbc

def get_db_connection():
    drivers_to_try = [
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
        "ODBC Driver 18 for SQL Server"
    ]
    
    passwords_to_try = [
        "PasswordGS15",
        "PasswordGS2017",
        "PasswordGS2021",
        "Password01"
    ]

    for password in passwords_to_try:
        print(f"Trying password: {password}...")
        for driver in drivers_to_try:
            print(f"  Trying driver: {{{driver}}}...")
            try:
                # Reconstruct connection string
                conn_str = (
                    f"Driver={{{driver}}};"
                    f"Server={DB_SERVER};"
                    f"Database={DB_NAME};"
                    f"UID={DB_USER};"
                    f"PWD={password};"
                    "Encrypt=no;"
                    "TrustServerCertificate=yes;"
                    "LoginTimeout=5;"
                )
                conn = pyodbc.connect(conn_str)
                print(f"✅ Connection successful with {driver} and password ending in ...{password[-4:]}!")
                return conn
            except Exception as e:
                error_msg = str(e)
                # If login failed, no point trying other drivers with SAME password, move to next password (unless it's driver missing)
                if "Login failed" in error_msg:
                    print(f"  ❌ Login failed for {DB_USER} with current password.")
                    break # Break driver loop to try next password
                elif "Data source name not found" in error_msg or "Driver not found" in error_msg:
                    print(f"  ⚠️ Driver {driver} not found.")
                    continue # Try next driver
                else:
                    print(f"  ❌ Error connecting via {driver}: {error_msg}")
                    # Continue attempting other drivers just in case
        
    print("❌ All connection attempts failed.")
    return None

def main():
    excel_file = "products.xlsx"  # Default filename, can be changed or passed as arg
    
    # Check if a file argument is provided
    if len(sys.argv) > 1:
        excel_file = sys.argv[1]

    if not os.path.exists(excel_file):
        print(f"Error: Excel file '{excel_file}' not found.")
        print("Usage: python get_trust_ids_from_excel.py [excel_file_path]")
        return

    print(f"Reading from {excel_file}...")
    try:
        df = pd.read_excel(excel_file)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Check for likely column names
    col_name = "项目名称"
    if col_name not in df.columns:
        if len(df.columns) > 0:
            print(f"Warning: Column '{col_name}' not found. Using first column '{df.columns[0]}' instead.")
            col_name = df.columns[0]
        else:
            print("Error: Excel file appears to be empty or has no columns.")
            return

    project_names = df[col_name].dropna().astype(str).tolist()
    print(f"Found {len(project_names)} project names.")

    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()
    
    trust_ids = []
    found_count = 0
    missing_count = 0

    print("Querying database...")
    missing_items = []
    for name in project_names:
        # Clean the name if necessary (trim whitespace)
        clean_name = name.strip()
        
        # Simple exact match query first
        query = "SELECT TrustId FROM [PortfolioManagement].[TrustManagement].[Trust] WHERE TrustName = ?"
        cursor.execute(query, clean_name)
        row = cursor.fetchone()
        
        if row:
            trust_id = str(row[0])
            trust_ids.append(trust_id)
            found_count += 1
            # print(f"Found: {clean_name} -> {trust_id}")
        else:
            print(f"⚠️  ALARM: TrustName not found: {clean_name}")
            missing_items.append(clean_name)
            missing_count += 1

    conn.close()

    if missing_items:
        print("\n" + "="*30)
        print(f"FAILED TO FIND {len(missing_items)} ITEMS:")
        for item in missing_items:
            print(f" - {item}")
        print("="*30 + "\n")


    output_file = "trustids.txt"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            for tid in trust_ids:
                f.write(tid + "\n")
        print(f"\nProcessing complete.")
        print(f"Found: {found_count}")
        print(f"Missing: {missing_count}")
        print(f"TrustIDs written to {output_file}")
    except Exception as e:
        print(f"Error writing output file: {e}")

if __name__ == "__main__":
    main()
