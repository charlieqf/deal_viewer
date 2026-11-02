import hashlib,json,os,secrets,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pyodbc,pymupdf
from risk_repository import append_event
from risk_rules import business_day

root=Path('/data/dealviewer-issuer-production/risk-test')
config=json.loads((root/'test-connection.local.json').read_text());cn=pyodbc.connect(config['odbc'])
assert cn.execute('SELECT DB_NAME()').fetchval()=='DealViewerRiskTest_20261102'
i=cn.execute("INSERT DV.Issuer(LegalName,DisplayName,InstitutionType) OUTPUT inserted.IssuerId VALUES(N'页面验收银行',N'页面验收','bank')").fetchval()
pdf=pymupdf.open();page=pdf.new_page();page.insert_text((40,60),'Acceptance original: Issuer long-term rating downgraded to AA+.');data=pdf.tobytes();pdf.close();digest=hashlib.sha256(data).digest()
archive=root/'archive';archive.mkdir(exist_ok=True);(archive/(digest.hex()+'.pdf')).write_bytes(data)
file=cn.execute('SELECT FileId FROM DV.IssuerFile WHERE Sha256=?',digest).fetchval()
if not file:file=cn.execute("INSERT DV.IssuerFile(Sha256,StorageKey,ByteSize,PageCount,ValidationStatus,StorageBackendCode,ArchiveStatus) OUTPUT inserted.FileId VALUES(?,?,?,1,'verified','r760_filesystem','archived')",digest,digest.hex()+'.pdf',len(data)).fetchval()
doc=cn.execute("INSERT DV.IssuerDocument(IssuerId,FileId,Title,DocumentType) OUTPUT inserted.DocumentId VALUES(?,?,N'验收评级原文','issuer_rating')",i,file).fetchval()
facts={'agency':'验收评级机构','grade':'AA+','previous_grade':None,'change_direction':'下调','event_date':business_day().isoformat(),
       'evidence':{'change_direction':{'page':1,'quote':'Issuer long-term rating downgraded to AA+.'}}}
event=append_event(cn,i,'页面验收银行','rating',facts,{1:'Issuer long-term rating downgraded to AA+.'},'https://example.test/rating',digest.hex(),doc)
cn.execute("INSERT DV.RiskCheckLog(IssuerId,Source,CheckKey,CheckedAt,QueryStart,QueryEnd,Status) VALUES(?,'chinamoney_rating',?,DATEADD(day,-15,SYSUTCDATETIME()),DATEADD(month,-24,CONVERT(date,SYSUTCDATETIME())),CONVERT(date,SYSUTCDATETIME()),'success')",i,hashlib.sha256(str(i).encode()).digest())
cn.commit();cn.close()
key=secrets.token_urlsafe(30)
for name,body in [('web.local.json',{'odbc':config['odbc'],'database':config['database']}),('review.local.json',{'odbc':config['odbc'],'review_key':key}),('browser.local.json',{'review_key':key,'issuer_id':i,'event_id':event,'document_id':doc,'digest':digest.hex()})]:
 p=root/name;p.write_text(json.dumps(body));os.chmod(p,0o600)
print(json.dumps({'issuer_id':i,'event_id':event,'document_id':doc}))
