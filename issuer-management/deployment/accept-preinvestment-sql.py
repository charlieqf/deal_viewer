"""Exercise migration, scoring and corrections only in the isolated SQL 2014 database."""
import hashlib,json,re,sys,uuid
from pathlib import Path
from datetime import timedelta
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pyodbc
from risk_repository import append_event,review,detail,rows
from risk_rules import business_day,window_start
from preinvestment import KINDS,REQUIRED
from issuer_review import ReviewError

root=Path('/data/dealviewer-issuer-production/risk-test')
cn=pyodbc.connect(json.loads((root/'test-connection.local.json').read_text())['odbc'],autocommit=False)
assert tuple(cn.execute("SELECT DB_NAME(),IS_SRVROLEMEMBER('sysadmin'),HAS_DBACCESS('PortfolioManagement')").fetchone())==('DealViewerRiskTest_20261102',0,0)
assert cn.execute("SELECT CONVERT(varchar(32),SERVERPROPERTY('ProductVersion'))").fetchval().startswith('12.')
sql=(Path(__file__).resolve().parents[1]/'production_migration_010_preinvestment.sql').read_text(encoding='utf-8')
for part in re.split(r'(?m)^GO\s*$',sql):
    if part.strip():cn.execute(part)
cn.commit()
checks=[];tag=uuid.uuid4().hex;day=business_day()
try:
    i=cn.execute("INSERT DV.Issuer(LegalName,DisplayName,InstitutionType) OUTPUT inserted.IssuerId VALUES(?,N'投前隔离验收','bank')",'preinvestment-'+tag).fetchval()
    def score():return cn.execute('SELECT SuggestedDeduction FROM DV.vw_IssuerPreInvestmentRiskSummary WHERE IssuerId=?',i).fetchval()
    def correct(e,f,status='confirmed',when=None):
        old=rows(cn,'SELECT * FROM DV.RiskEvent WHERE RiskEventId=?',e)[0]
        return review(cn,dict(operation='risk_event',id=e,version=bytes(old['RowVersion']).hex(),fields=f,status=status,
            event_date=(when or day).isoformat(),reviewer='隔离验收',reason='验证实际条件及计分隔离',request_id=uuid.uuid4().hex))
    for kind in KINDS:
        f=dict(has_event=True,issuer_applicable=True,product_name='验收ABS',event_date=day.isoformat(),
               evidence={'review':{'page':1,'quote':'Synthetic acceptance evidence: actual event.'}},**{k:True for k in REQUIRED[kind]})
        e=append_event(cn,i,'测试银行',kind,f,{},'https://example.test/'+kind,hashlib.sha256((tag+kind).encode()).hexdigest())
        assert cn.execute('SELECT Status FROM DV.RiskEvent WHERE RiskEventId=?',e).fetchval()=='pending'
        for field in REQUIRED[kind]:
            try:correct(e,dict(f,**{field:False}))
            except ReviewError:pass
            else:raise AssertionError('incomplete_conditions_accepted')
        correct(e,f);assert score()==5
        assert cn.execute('SELECT SuggestedScore FROM DV.vw_IssuerRiskSummary WHERE IssuerId=?',i).fetchval()==15
        checks.append(kind+'_confirmed_capped_and_separate')
        correct(e,f,when=window_start(day)-timedelta(days=1));assert score()==0
        correct(e,f,when=window_start(day));assert score()==5
        correct(e,f,status='excluded');assert score()==0
    f=dict(has_event=False,unresolved=False,evidence={'conclusion':{'page':1,'quote':'Only contractual provisions; no actual event.'}})
    assert append_event(cn,i,'测试银行','servicer_failure',f,{1:f['evidence']['conclusion']['quote']},'https://example.test/no-event',hashlib.sha256(tag.encode()).hexdigest()) is None
    checks+=['24_month_inclusive_boundary','correction_recalculates','no_event_archive_only']
    d=detail(cn,i);assert d['preinvestment_summary']['SuggestedDeduction']==0
    assert all(e.get('CurrentState',{}).get('code')=='unknown' for e in d['events'])
    checks+=['unknown_effectiveness_not_false','source_status_api']
finally:cn.rollback();cn.close()
result={'passed':True,'database':'DealViewerRiskTest_20261102','sql_major':12,'production_access':False,'checks':checks}
(root/'preinvestment-sql-acceptance.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(result,ensure_ascii=False))
