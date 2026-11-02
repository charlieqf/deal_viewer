"""Provision a separately owned SQL 2014 acceptance DB on the authorized instance.

No production data is copied. Dedicated login can access this database only.
Run with the existing admin secret mounted; generated credentials stay private.
"""
import json
import os
from pathlib import Path
import re
import secrets
import pyodbc
from dealviewer_runtime import secret

DATABASE='DealViewerRiskTest_20261102'
LOGIN='DealViewerRiskTest_20261102'
PURPOSE='DealViewer risk V2.3 isolated acceptance 2026-11-02'
ROOT=Path(os.environ.get('RISK_TEST_ROOT','/data/dealviewer-issuer-production/risk-test'))
SRC=Path(__file__).resolve().parents[1]


def main():
    ROOT.mkdir(parents=True,exist_ok=True)
    config=ROOT/'test-connection.local.json'
    original=secret('SQL_ODBC_ABN_PRODUCTS')
    cn=pyodbc.connect(original,autocommit=True,timeout=15)
    identity=cn.execute("SELECT CONVERT(varchar(128),SERVERPROPERTY('ServerName')),CONVERT(varchar(32),SERVERPROPERTY('ProductVersion'))").fetchone()
    assert identity[0]=='CNGY_6_6143_DEA\\MSSQL' and identity[1].startswith('12.'), 'unexpected_instance'
    if not cn.execute('SELECT DB_ID(?)',DATABASE).fetchval():
        # Existing data volume E: was measured full. Keep this bounded test on C:.
        free=dict(cn.execute('EXEC master.dbo.xp_fixeddrives').fetchall())
        if free.get('C',0)<512:raise RuntimeError('test_volume_requires_512MB_free')
        cn.execute("EXEC master.dbo.xp_create_subdir N'C:\\temp\\DealViewerRiskTest'")
        cn.execute("CREATE DATABASE ["+DATABASE+"] ON PRIMARY (NAME=N'RiskTestData',FILENAME=N'C:\\temp\\DealViewerRiskTest\\risk.mdf',SIZE=32MB,MAXSIZE=128MB,FILEGROWTH=16MB) LOG ON (NAME=N'RiskTestLog',FILENAME=N'C:\\temp\\DealViewerRiskTest\\risk.ldf',SIZE=16MB,MAXSIZE=64MB,FILEGROWTH=8MB)")
        cn.execute('ALTER DATABASE ['+DATABASE+'] SET RECOVERY SIMPLE')
        cn.execute('USE ['+DATABASE+']')
        cn.execute("EXEC sys.sp_addextendedproperty @name=N'RiskTestPurpose',@value=?",PURPOSE)
    else:
        cn.execute('USE ['+DATABASE+']')
        assert cn.execute("SELECT CONVERT(nvarchar(256),value) FROM sys.extended_properties WHERE class=0 AND name='RiskTestPurpose'").fetchval()==PURPOSE
    if not config.exists():
        assert not cn.execute('SELECT principal_id FROM sys.server_principals WHERE name=?',LOGIN).fetchval(), 'login_without_private_config'
        password=secrets.token_urlsafe(40)+'Aa1!'
        cn.execute("DECLARE @p nvarchar(128)=?; DECLARE @s nvarchar(max)=N'CREATE LOGIN ["+LOGIN+"] WITH PASSWORD='+QUOTENAME(@p,'''')+N', DEFAULT_DATABASE=["+DATABASE+"]'; EXEC(@s)",password)
        cn.execute('CREATE USER ['+LOGIN+'] FOR LOGIN ['+LOGIN+']')
        cn.execute('ALTER ROLE db_owner ADD MEMBER ['+LOGIN+']')
        text=re.sub(r'(?i)(^|;)(UID|USER ID)=[^;]*',lambda m:m[1]+'UID='+LOGIN,original)
        text=re.sub(r'(?i)(^|;)(PWD|PASSWORD)=[^;]*',lambda m:m[1]+'PWD='+password,text)
        text=re.sub(r'(?i)(^|;)(DATABASE|INITIAL CATALOG)=[^;]*',lambda m:m[1]+'DATABASE='+DATABASE,text)
        if 'DATABASE='+DATABASE not in text: text+=';DATABASE='+DATABASE
        config.write_text(json.dumps({'odbc':text,'database':DATABASE,'purpose':PURPOSE}))
        os.chmod(config,0o600)
    cn.close()
    cn=pyodbc.connect(json.loads(config.read_text())['odbc'],autocommit=False,timeout=15)
    permissions=tuple(cn.execute("SELECT DB_NAME(),IS_SRVROLEMEMBER('sysadmin'),HAS_DBACCESS('PortfolioManagement')").fetchone())
    assert permissions==(DATABASE,0,0),permissions
    if not cn.execute("SELECT OBJECT_ID('DV.Issuer')").fetchval():
        for name in ('pilot_schema.sql','pilot_migration_002_storage.sql','pilot_migration_003_evidence.sql','production_migration_004.sql','production_migration_005_rating.sql','production_migration_007_published_profits.sql'):
            for batch in re.split(r'(?m)^GO\s*$',(SRC/name).read_text(encoding='utf-8')):
                if batch.strip():cn.execute(batch)
        cn.execute('''CREATE TABLE DV.tbl_view_Products(TrustId bigint,TrustCode nvarchar(200),TrustNameShort nvarchar(200),
           TrustName nvarchar(512),ProductSubject nvarchar(512),StatusTrust varchar(32),TrustStatus nvarchar(128),TrustStartDate nvarchar(64),
           MarketPlaceName nvarchar(64),MarketCategoryName nvarchar(64),AssetTypeName nvarchar(64))''')
        cn.commit()
    for batch in re.split(r'(?m)^GO\s*$',(SRC/'production_migration_008_risk.sql').read_text(encoding='utf-8')):
        if batch.strip():cn.execute(batch)
    for batch in re.split(r'(?m)^GO\s*$',(SRC/'production_migration_009_product_mapping.sql').read_text(encoding='utf-8')):
        if batch.strip():cn.execute(batch)
    cn.commit()
    print(json.dumps({'database':DATABASE,'version':identity[1],'sysadmin':False,'production_access':False,'schema_ready':True}))
    cn.close()

if __name__=='__main__':main()
