"""Concurrent writes use only the isolated SQL 2014 acceptance database."""
import hashlib
import json
from pathlib import Path
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pyodbc
from risk_repository import append_event
from risk_rules import business_day

config=json.loads(Path('/data/dealviewer-issuer-production/risk-test/test-connection.local.json').read_text())
def connect():
    cn=pyodbc.connect(config['odbc'],autocommit=False)
    assert tuple(cn.execute("SELECT DB_NAME(),IS_SRVROLEMEMBER('sysadmin'),HAS_DBACCESS('PortfolioManagement')").fetchone())==('DealViewerRiskTest_20261102',0,0)
    assert cn.execute("SELECT CONVERT(varchar(32),SERVERPROPERTY('ProductVersion'))").fetchval().startswith('12.')
    return cn

tag=uuid.uuid4().hex
cn=connect()
issuer=cn.execute("INSERT DV.Issuer(LegalName,DisplayName,InstitutionType) OUTPUT inserted.IssuerId VALUES(?,?,'bank')",'parallel-'+tag,'parallel acceptance').fetchval()
cn.commit()

def write(index):
    db=connect()
    try:
        identity=tag+'-'+str(index)
        facts=dict(event_date=business_day().isoformat(),has_default=True,debtor='parallel-'+tag,issuer_role='debtor',debt_id='acceptance-'+tag,action='unpaid')
        result=append_event(db,issuer,'parallel-'+tag,'default',facts,{},'https://example.test/'+identity,hashlib.sha256(identity.encode()).hexdigest())
        db.commit()
        return result
    finally:db.close()

try:
    with ThreadPoolExecutor(max_workers=4) as pool:
        ids=list(pool.map(write,range(12)))
    # Same fact from twelve documents must keep one event and all twelve sources.
    assert len(set(ids))==1
    evidence=json.loads(cn.execute('SELECT EvidenceJson FROM DV.RiskEvent WHERE RiskEventId=?',ids[0]).fetchval())
    assert len(evidence)==12
    assert cn.execute('SELECT Status FROM DV.RiskEvent WHERE RiskEventId=?',ids[0]).fetchval()=='pending'
    print(json.dumps({'isolated_sql_2014':True,'workers':4,'writes':12,'events':1,'sources':12,'status':'pending'}))
finally:
    cn.execute('DELETE DV.RiskEvent WHERE IssuerId=?',issuer)
    cn.execute('DELETE DV.Issuer WHERE IssuerId=?',issuer)
    cn.commit();cn.close()
