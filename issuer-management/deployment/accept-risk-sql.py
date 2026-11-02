"""A01-A19 acceptance on the private, non-production SQL 2014 test database."""
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid
from datetime import timedelta
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pyodbc
from risk_rules import business_day, window_start, classify
from risk_repository import append_event, review, rows, detail, sync_mappings
from test_risk import penalty, RiskRulesTests

ROOT=Path(os.environ.get('RISK_TEST_ROOT','/data/dealviewer-issuer-production/risk-test'))


def main():
    config=json.loads((ROOT/'test-connection.local.json').read_text())
    cn=pyodbc.connect(config['odbc'],autocommit=False)
    assert tuple(cn.execute("SELECT DB_NAME(),IS_SRVROLEMEMBER('sysadmin'),HAS_DBACCESS('PortfolioManagement')").fetchone())==('DealViewerRiskTest_20261102',0,0)
    assert cn.execute("SELECT CONVERT(varchar(32),SERVERPROPERTY('ProductVersion'))").fetchval().startswith('12.')
    result={};day=business_day();run=uuid.uuid4().hex[:8]
    def issuer(name='测试银行'):
        return cn.execute("INSERT DV.Issuer(LegalName,DisplayName,InstitutionType) OUTPUT inserted.IssuerId VALUES(?,?,'bank')",name+run,name).fetchval()
    def event(i,category,facts,tag,pages=None,url=None):
        digest=hashlib.sha256((run+tag).encode()).hexdigest()
        name=cn.execute('SELECT LegalName FROM DV.Issuer WHERE IssuerId=?',i).fetchval()
        if facts.get('party')=='测试银行':facts=dict(facts,party=name)
        return append_event(cn,i,name,category,dict(event_date=day.isoformat(),**facts),pages or {},url or 'https://example.test/'+tag,digest)
    def confirm(e,status='confirmed',**extra):
        old=rows(cn,'SELECT * FROM DV.RiskEvent WHERE RiskEventId=?',e)[0]
        p=dict(operation='risk_event',id=e,version=bytes(old['RowVersion']).hex(),status=status,
               event_date=str(old['EventDate']) if old['EventDate'] else None,reviewer='SQL验收',reason='核对验收原文及适用口径',request_id=uuid.uuid4().hex,**extra)
        review(cn,p)
    def score(i):return cn.execute('SELECT SuggestedScore FROM DV.vw_IssuerRiskSummary WHERE IssuerId=?',i).fetchval()
    def state(e):return cn.execute('SELECT Status FROM DV.RiskEvent WHERE RiskEventId=?',e).fetchval()
    i=issuer();f=penalty('110000000');e=event(i,'penalty',f,'A01');confirm(e);assert score(i)==10;result['A01']='pass'
    for label,facts in [('A02',penalty('80000000',fine_confiscation_total='110000000')),('A03',penalty('100000000')),('A15',penalty(fine_confiscation_total='90000000'))]:
        i=issuer();e=event(i,'penalty',facts,label);assert state(e)=='below_threshold' and score(i)==15;result[label]='pass'
    i=issuer();f=penalty('120000000');f['party_type']='branch';e=event(i,'penalty',f,'A04');confirm(e,'excluded');assert score(i)==15;result['A04']='pass'
    i=issuer();e=event(i,'rating',{'agency':'测试评级','grade':'AA+','previous_grade':None,'change_direction':'下调','evidence':{'change_direction':{'page':1,'quote':'主体评级下调至AA+'}}},'A05');confirm(e);assert score(i)==5;result['A05']='pass'
    old=json.loads(cn.execute('SELECT RawExtractionJson FROM DV.RiskEvent WHERE RiskEventId=?',e).fetchval());assert old['previous_grade'] is None
    before=cn.execute('SELECT COUNT(*) FROM DV.IssuerChangeLog').fetchval();confirm(e,'excluded');assert score(i)==15 and cn.execute('SELECT COUNT(*) FROM DV.IssuerChangeLog').fetchval()==before+1
    assert json.loads(cn.execute('SELECT RawExtractionJson FROM DV.RiskEvent WHERE RiskEventId=?',e).fetchval())==old;result['A14']='pass'
    i=issuer();e=event(i,'rating',{'change_direction':'未知','action':'outlook_change'},'A06');confirm(e,'excluded');assert score(i)==15;result['A06']='pass'
    i=issuer();e=event(i,'default',{'has_default':True,'debtor':'测试银行','issuer_role':'debtor'},'A07d');confirm(e);e=event(i,'penalty',penalty('110000000'),'A07p');confirm(e);assert score(i)==0;result['A07']='pass'
    i=issuer();facts=penalty('110000000');e=event(i,'penalty',facts,'A08a',url='https://qcc.example.test/1');e2=event(i,'penalty',facts,'A08b',url='https://official.example.test/1');assert e==e2 and len(json.loads(cn.execute('SELECT EvidenceJson FROM DV.RiskEvent WHERE RiskEventId=?',e).fetchval()))==2;result['A08']='pass'
    i=issuer();key=hashlib.sha256((run+'check').encode()).digest()
    cn.execute("INSERT DV.RiskCheckLog(IssuerId,Source,CheckKey,CheckedAt,QueryStart,QueryEnd,Status,FailureReason) VALUES(?,'qcc',?,SYSUTCDATETIME(),?,?,'failed',N'凭据尚未开通')",i,key,window_start(day),day)
    assert score(i)==15 and detail(cn,i)['sources'][0]['latest']['Status']=='failed';result['A09']='pass'
    cn.execute("INSERT DV.RiskCheckLog(IssuerId,Source,CheckKey,CheckedAt,QueryStart,QueryEnd,Status) VALUES(?,'qcc',?,DATEADD(day,-15,SYSUTCDATETIME()),?,?,'success')",i,hashlib.sha256(key).digest(),window_start(day),day-timedelta(days=15))
    assert detail(cn,i)['sources'][0]['red'];result['A10']='pass'
    a=issuer('大众汽车金融（中国）有限公司');b=issuer('另一发起人');pid=900000000+int(run[:6],16)
    cn.execute("INSERT DV.tbl_view_Products(TrustId,ProductSubject,MarketPlaceName,MarketCategoryName) VALUES(?,?,N'银行间债券市场',N'信贷资产证券化')",pid,'大众汽车金融(中国)有限公司'+run)
    inventory=[dict(id=a,name='大众汽车金融（中国）有限公司'+run),dict(id=b,name='另一发起人'+run)]
    sync_mappings(cn,inventory);m=rows(cn,'SELECT * FROM DV.ProductIssuerMapping WHERE ProductId=?',pid)[0];assert m['IssuerId']==a
    sync_mappings(cn,inventory)
    assert bytes(cn.execute('SELECT RowVersion FROM DV.ProductIssuerMapping WHERE ProductId=?',pid).fetchval())==bytes(m['RowVersion'])
    review(cn,dict(operation='product_mapping',id=pid,issuer_id=b,version=bytes(m['RowVersion']).hex(),reviewer='SQL验收',reason='按项目原文修正发起人',request_id=uuid.uuid4().hex));sync_mappings(cn,inventory)
    assert cn.execute('SELECT IssuerId FROM DV.ProductIssuerMapping WHERE ProductId=?',pid).fetchval()==b;result['A11']='pass'
    RiskRulesTests('test_two_weeks_retry_and_month_store_untouched').test_two_weeks_retry_and_month_store_untouched();result['A12']='pass'
    i=issuer();e=event(i,'rating',{'change_direction':'下调'},'A13');cn.execute('UPDATE DV.RiskEvent SET EventDate=? WHERE RiskEventId=?',window_start(day)-timedelta(days=1),e);confirm(e);assert score(i)==15;result['A13']='pass'
    i=issuer();name=cn.execute('SELECT LegalName FROM DV.Issuer WHERE IssuerId=?',i).fetchval();text='甲公司实际违约。'+name+'仅作为主承销商代发公告。'
    f=dict(has_default=True,debtor='甲公司',issuer_role='lead_underwriter',unresolved=False,own_obligation_abnormal=False,evidence={'debtor':{'page':1,'quote':'甲公司实际违约'},'issuer_role':{'page':1,'quote':name+'仅作为主承销商代发公告'}})
    e=event(i,'default',f,'A16',pages={1:text});assert state(e)=='other_debtor' and score(i)==15;result['A16']='pass'
    RiskRulesTests('test_title_rejection_does_not_download').test_title_rejection_does_not_download();result['A17']='pass'
    e=event(i,'default',dict(f,own_obligation_abnormal=True),'A18',pages={1:text});assert state(e)=='pending' and score(i)==15;result['A18']='pass'
    count=cn.execute('SELECT COUNT(*) FROM DV.RiskEvent').fetchval();f=dict(has_default=False,own_obligation_abnormal=False,unresolved=False,evidence={'conclusion':{'page':1,'quote':'已按期兑付，无违约事实'}})
    assert event(i,'default',f,'A19',pages={1:'已按期兑付，无违约事实'}) is None and cn.execute('SELECT COUNT(*) FROM DV.RiskEvent').fetchval()==count;result['A19']='pass'
    cn.commit();cn.close()
    (ROOT/'sql-acceptance.json').write_text(json.dumps({'run':run,'database':config['database'],'cases':dict(sorted(result.items())),'passed':len(result)},ensure_ascii=False,indent=2))
    print(json.dumps({'passed':len(result),'cases':sorted(result)}))

if __name__=='__main__':main()
