"""Read-only production WSGI application. Only the dedicated module reader secret is mounted."""
import json
from contextlib import contextmanager
import mimetypes
import os
from pathlib import Path
import re
import io
import threading
import hmac
import time
from urllib.parse import parse_qs,quote
from decimal import Decimal
import pyodbc

WEB=Path(__file__).parent/'web';ARCHIVE=Path(os.environ.get('ISSUER_ARCHIVE_ROOT','/archive'))
PAGE_LOCK=threading.BoundedSemaphore(1)
REVIEW_LOCK=threading.Lock()
REVIEW_FAILURES={}

def review_response(env,start):
    from issuer_review import apply_review,ReviewError
    if env['REQUEST_METHOD']!='POST':
        return respond(start,'405 Method Not Allowed',{'error':'请使用核对表单提交。'})
    origin=env.get('HTTP_ORIGIN','')
    # Browser submissions must come from this origin; no CORS is granted.
    from urllib.parse import urlsplit
    if not origin or urlsplit(origin).netloc!=env.get('HTTP_HOST',''):
        return respond(start,'403 Forbidden',{'error':'请从当前页面提交核对。'})
    if not env.get('CONTENT_TYPE','').startswith('application/json'):
        return respond(start,'415 Unsupported Media Type',{'error':'仅接受 JSON 表单。'})
    try:
        length=int(env.get('CONTENT_LENGTH','0'))
        if not 0<length<=65536:raise ValueError()
        payload=json.loads(env['wsgi.input'].read(length))
        if not isinstance(payload,dict):raise ValueError()
    except (ValueError,TypeError):
        return respond(start,'400 Bad Request',{'error':'表单格式无效。'})
    config_path=os.environ.get('ISSUER_REVIEW_CONFIG')
    if not config_path:return respond(start,'503 Service Unavailable',{'error':'复核入口尚未配置。'})
    config=json.loads(Path(config_path).read_text())
    remote=env.get('REMOTE_ADDR','local');now=time.monotonic()
    with REVIEW_LOCK:
        attempts=[t for t in REVIEW_FAILURES.get(remote,[]) if now-t<300]
        if len(attempts)>=12:return respond(start,'429 Too Many Requests',{'error':'口令错误次数过多，请稍后再试。'})
        if not hmac.compare_digest(str(env.get('HTTP_X_REVIEW_KEY','')),config['review_key']):
            REVIEW_FAILURES[remote]=attempts+[now]
            return respond(start,'403 Forbidden',{'error':'复核口令不正确。'})
        REVIEW_FAILURES.pop(remote,None)
    cn=None
    try:
        cn=pyodbc.connect(config['odbc'],timeout=10,autocommit=False);cn.timeout=25
        dry_run=payload.pop('dry_run',False) is True
        if payload.get('operation') in ('risk_event','product_mapping','risk_add'):
            from risk_repository import review
            result=review(cn,payload)
        else:result=apply_review(cn,payload)
        if dry_run:cn.rollback();result={'validated':True,'saved':False}
        else:cn.commit()
        return respond(start,'200 OK',result)
    except ReviewError as e:
        if cn:cn.rollback()
        return respond(start,e.status,{'error':str(e)})
    except Exception as e:
        if cn:cn.rollback()
        print(json.dumps({'event':'issuer_review_error','type':type(e).__name__}),flush=True)
        return respond(start,'503 Service Unavailable',{'error':'保存失败，事务已回滚；请重试。'})
    finally:
        if cn:cn.close()

def page_response(env,start,cn,id,q):
    import pypdfium2 as pdfium
    raw=q.get('page',['1'])[0]
    if not re.fullmatch(r'\d{1,5}',raw):return respond(start,'400 Bad Request',{'error':'invalid_page'})
    rows=query(cn,"SELECT f.StorageKey,f.PageCount FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId WHERE d.DocumentId=? AND d.DocumentStatus='active' AND f.ArchiveStatus='archived'",id)
    if not rows or not re.fullmatch(r'[a-f0-9]{64}\.pdf',rows[0]['StorageKey']):return respond(start,'404 Not Found',{'error':'not_found'})
    number=int(raw)
    if not 1<=number<=rows[0]['PageCount']:return respond(start,'400 Bad Request',{'error':'invalid_page'})
    if not PAGE_LOCK.acquire(timeout=10):return respond(start,'503 Service Unavailable',{'error':'render_busy'})
    try:
        pdf=pdfium.PdfDocument(ARCHIVE/rows[0]['StorageKey'])
        try:
            page=pdf[number-1]
            scale=min(1.8,1600/max(page.get_size()))
            bitmap=page.render(scale=scale);picture=bitmap.to_pil();out=io.BytesIO();picture.save(out,format='PNG');picture.close();bitmap.close();page.close()
        finally:pdf.close()
        return respond(start,'200 OK',out.getvalue(),'image/png',headers=[('X-PDF-Page',str(number))],head=env['REQUEST_METHOD']=='HEAD')
    finally:PAGE_LOCK.release()
SELECTED="""WITH ranked AS (SELECT r.FinancialRecordId,r.IssuerId,r.RecordStatus,s.PeriodStart,s.PeriodEnd,s.AccountingBasisCode,
ROW_NUMBER() OVER(PARTITION BY r.IssuerId,s.PeriodEnd ORDER BY r.FinancialRecordId DESC) n
FROM DV.IssuerFinancialRecord r JOIN DV.IssuerFinancialSeries s ON s.SeriesId=r.SeriesId WHERE r.RecordStatus<>'withdrawn') """
@contextmanager
def connect():
    config=json.loads(Path(os.environ['ISSUER_WEB_CONFIG']).read_text())
    cn=pyodbc.connect(config['odbc'],timeout=10,autocommit=True);cn.timeout=25
    try:yield cn
    finally:cn.close()
def query(cn,sql,*args):
    cur=cn.execute(sql,*args);names=[x[0] for x in cur.description]
    def serial(x,key):
        if isinstance(x,Decimal) or (isinstance(x,int) and key.endswith('Id')):return str(x)
        if hasattr(x,'isoformat'):return x.isoformat()
        return x
    return [{k:serial(v,k) for k,v in zip(names,row)} for row in cur.fetchall()]
def respond(start,status,body,mime='application/json; charset=utf-8',headers=None,head=False):
    if not isinstance(body,bytes):body=json.dumps(body,ensure_ascii=False).encode()
    start(status,[('Content-Type',mime),('Content-Length',str(len(body))),('Cache-Control','no-store'),('X-Content-Type-Options','nosniff'),('X-Frame-Options','SAMEORIGIN')]+(headers or []))
    return [] if head else [body]
def document_response(env,start,cn,id):
    rows=query(cn,"SELECT d.Title,f.StorageKey,f.ArchiveStatus,f.StorageBackendCode,f.ByteSize,CONVERT(varchar(64),f.Sha256,2) Sha256 FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId WHERE d.DocumentId=? AND d.DocumentStatus='active'",id)
    if len(rows)!=1:return respond(start,'404 Not Found',{'error':'not_found'})
    doc=rows[0];key=doc['StorageKey']
    if doc['ArchiveStatus']!='archived' or doc['StorageBackendCode'] not in ('r760_filesystem','dealviewer_filesystem') or not re.fullmatch('[a-f0-9]{64}\\.pdf',key):return respond(start,'404 Not Found',{'error':'not_archived'})
    path=ARCHIVE/key
    if not path.is_file() or path.stat().st_size!=doc['ByteSize']:return respond(start,'503 Service Unavailable',{'error':'archive_unavailable'})
    size=doc['ByteSize'];first=0;last=size-1;status='200 OK';headers=[]
    value=env.get('HTTP_RANGE')
    if value:
        m=re.fullmatch(r'bytes=(\d*)-(\d*)',value)
        if not m or not any(m.groups()):return respond(start,'416 Range Not Satisfiable',b'',headers=[('Content-Range',f'bytes */{size}')])
        if not m[1]:first=max(0,size-int(m[2]))
        else:first=int(m[1]);last=min(size-1,int(m[2])) if m[2] else size-1
        if first>=size or first>last:return respond(start,'416 Range Not Satisfiable',b'',headers=[('Content-Range',f'bytes */{size}')])
        status='206 Partial Content';headers.append(('Content-Range',f'bytes {first}-{last}/{size}'))
    disposition='attachment' if parse_qs(env.get('QUERY_STRING','')).get('download')==['1'] else 'inline'
    headers += [('Content-Type','application/pdf'),('Content-Length',str(last-first+1)),('Accept-Ranges','bytes'),('ETag','"'+doc['Sha256'].lower()+'"'),('X-Content-Type-Options','nosniff'),('Content-Disposition',disposition+"; filename=report.pdf; filename*=UTF-8''"+quote(doc['Title'],safe=''))]
    start(status,headers)
    if env['REQUEST_METHOD']=='HEAD':return []
    def stream():
        with path.open('rb') as f:
            f.seek(first);remaining=last-first+1
            while remaining:
                block=f.read(min(65536,remaining))
                if not block:break
                remaining-=len(block);yield block
    return stream()

def library_response(cn,q):
    """Public metadata for archived originals only; never expose storage paths."""
    page=q.get('page',['1'])[0];issuer=q.get('issuer',[''])[0]
    year=q.get('year',[''])[0];kind=q.get('type',[''])[0];search=q.get('q',[''])[0].strip()
    if not re.fullmatch(r'[1-9]\d{0,5}',page):raise ValueError('invalid_page')
    if issuer and not re.fullmatch(r'\d{1,18}',issuer):raise ValueError('invalid_issuer')
    if year and not re.fullmatch(r'(19|20)\d{2}',year):raise ValueError('invalid_year')
    if kind not in ('','financial','rating') or len(search)>200:raise ValueError('invalid_filter')
    base=""" FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId
    JOIN DV.Issuer i ON i.IssuerId=d.IssuerId
    WHERE d.DocumentStatus='active' AND f.ArchiveStatus='archived'
    AND f.StorageBackendCode IN ('r760_filesystem','dealviewer_filesystem')"""
    date="CASE WHEN d.DocumentType='issuer_rating' THEN d.PublishedDate ELSE d.MainPeriodEnd END"
    where='';args=[]
    if issuer:where+=' AND d.IssuerId=?';args.append(int(issuer))
    if year:where+=' AND YEAR('+date+')=?';args.append(int(year))
    if kind:where+=(" AND d.DocumentType='issuer_rating'" if kind=='rating' else " AND d.DocumentType<>'issuer_rating'")
    if search:
        where+=' AND (CHARINDEX(?,d.Title)>0 OR CHARINDEX(?,i.LegalName)>0 OR CHARINDEX(?,i.DisplayName)>0)';args.extend([search]*3)
    total=query(cn,'SELECT COUNT(*) Total'+base+where,*args)[0]['Total']
    size=30;number=min(int(page),max(1,(total+size-1)//size))
    documents=query(cn,"""SELECT d.DocumentId,d.Title,d.DocumentType,d.MainPeriodEnd,d.PublishedDate,
    i.IssuerId,i.LegalName,i.MonitoringStatus,f.PageCount,f.ByteSize"""+base+where+
    ' ORDER BY COALESCE(d.PublishedDate,d.MainPeriodEnd) DESC,d.DocumentId DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY',
    *args,(number-1)*size,size)
    issuers=query(cn,'SELECT i.IssuerId,i.LegalName,COUNT(*) DocumentCount'+base+' GROUP BY i.IssuerId,i.LegalName ORDER BY i.LegalName')
    years=query(cn,'SELECT DISTINCT YEAR('+date+') Year'+base+' AND '+date+' IS NOT NULL ORDER BY Year DESC')
    return {'documents':documents,'total':total,'page':number,'page_size':size,'issuers':issuers,'years':[r['Year'] for r in years]}
def app(env,start):
    head=env['REQUEST_METHOD']=='HEAD'
    if env.get('PATH_INFO')=='/api.ashx' and parse_qs(env.get('QUERY_STRING','')).get('action')==['review']:
        return review_response(env,start)
    if env['REQUEST_METHOD'] not in ('GET','HEAD'):return respond(start,'405 Method Not Allowed',{'error':'read_only'})
    path=env.get('PATH_INFO','/')
    if path.startswith('/vendor/pdfjs/'):
        file=(WEB/path.lstrip('/')).resolve();vendor=(WEB/'vendor/pdfjs').resolve()
        if vendor not in file.parents or not file.is_file():return respond(start,'404 Not Found',{'error':'not_found'})
        mime='text/javascript' if file.suffix=='.mjs' else mimetypes.guess_type(file.name)[0] or 'application/octet-stream'
        return respond(start,'200 OK',file.read_bytes(),mime,head=head)
    if path in ('/','/index.html','/style.css','/app.js','/viewer.html','/viewer.js','/documents.html','/documents.css','/documents.js','/risk.html','/risk.js','/risk.css'):
        file=WEB/('index.html' if path=='/' else path[1:]);return respond(start,'200 OK',file.read_bytes(),(mimetypes.guess_type(file.name)[0] or 'text/plain')+'; charset=utf-8',head=head)
    if path not in ('/api.ashx','/health'):return respond(start,'404 Not Found',{'error':'not_found'})
    q=parse_qs(env.get('QUERY_STRING',''));action=q.get('action',['issuers'])[0];raw_id=q.get('id',['0'])[0]
    if not re.fullmatch(r'\d{1,18}',raw_id):return respond(start,'400 Bad Request',{'error':'invalid_id'})
    id=int(raw_id)
    try:
        with connect() as cn:
            if path=='/health':
                expected=json.loads(Path(os.environ['ISSUER_WEB_CONFIG']).read_text()).get('database','PortfolioManagement')
                assert cn.execute('SELECT DB_NAME()').fetchval()==expected
                cn.execute('SELECT TOP 1 MetricCode FROM DV.IssuerMetricDefinition').fetchone()
                release=Path(__file__).parent/'release.json'
                version=json.loads(release.read_text()).get('commit') if release.exists() else 'development'
                return respond(start,'200 OK',{'status':'ok','module':'issuer-management','commit':version},head=head)
            if action=='risk' and id>0:
                from risk_repository import detail as risk_detail
                if not cn.execute('SELECT IssuerId FROM DV.Issuer WHERE IssuerId=?',id).fetchval():
                    return respond(start,'404 Not Found',{'error':'not_found'})
                return respond(start,'200 OK',risk_detail(cn,id),head=head)
            if action=='risk-original' and id>0:
                event=query(cn,'SELECT EvidenceJson FROM DV.RiskEvent WHERE RiskEventId=?',id)
                digest=q.get('hash',[''])[0]
                if not event or not re.fullmatch('[0-9a-f]{64}',digest) or not any(e.get('sha256')==digest for e in json.loads(event[0]['EvidenceJson'])):
                    return respond(start,'404 Not Found',{'error':'not_found'})
                original=Path(os.environ.get('ISSUER_RISK_ROOT','/risk'))/'originals'/(digest+'.html')
                if not original.is_file():return respond(start,'404 Not Found',{'error':'not_found'})
                return respond(start,'200 OK',original.read_bytes(),'text/plain; charset=utf-8',head=head,
                               headers=[('Content-Disposition','attachment; filename="'+digest+'.html"')])
            if action=='mappings':
                data=query(cn,'SELECT m.*,i.LegalName FROM DV.ProductIssuerMapping m LEFT JOIN DV.Issuer i ON i.IssuerId=m.IssuerId ORDER BY m.ProductId DESC')
                for row in data:row['RowVersion']=bytes(row['RowVersion']).hex()
                return respond(start,'200 OK',data,head=head)
            if action in ('issuers','overview'):
                data=query(cn,"""SELECT i.IssuerId,i.LegalName,i.DisplayName,i.InstitutionType,i.MonitoringStatus,
                (SELECT COUNT(*) FROM DV.IssuerDocument d WHERE d.IssuerId=i.IssuerId AND d.DocumentStatus='active') DocumentCount,
                (SELECT MAX(MainPeriodEnd) FROM DV.IssuerDocument d WHERE d.IssuerId=i.IssuerId) LatestPeriod,
                (SELECT COUNT(DISTINCT s.PeriodEnd) FROM DV.IssuerFinancialSeries s WHERE s.IssuerId=i.IssuerId) PeriodCount,
                (SELECT TOP 1 t.Status FROM DV.IssuerProcessingTask t WHERE t.IssuerId=i.IssuerId AND t.RootTaskId IS NULL ORDER BY TaskId DESC) ProcessingStatus
                FROM DV.Issuer i ORDER BY i.IssuerId""")
                counts={r['IssuerId']:r['ProductCount'] for r in query(cn,
                    'SELECT IssuerId,COUNT(DISTINCT TrustId) ProductCount FROM DV.vw_IssuerNplProduct GROUP BY IssuerId')}
                for issuer in data:issuer['NplProductCount']=counts.get(issuer['IssuerId'],0)
                if action=='overview':
                    latest=SELECTED+""", latest AS (
                    SELECT *,ROW_NUMBER() OVER(PARTITION BY IssuerId ORDER BY PeriodEnd DESC) latest_n
                    FROM ranked WHERE n=1) """
                    data={'issuers':data,
                          'ratings':query(cn,"""WITH ranked AS (SELECT c.*,ROW_NUMBER() OVER(PARTITION BY c.IssuerId,c.Agency ORDER BY c.RatingDate DESC,c.RatingCandidateId DESC) n FROM DV.IssuerRatingCandidate c JOIN DV.IssuerDocument d ON d.DocumentId=c.DocumentId WHERE d.DocumentStatus='active') SELECT IssuerId,Agency,RatingDate,Grade,Outlook,RatingAction,ReviewStatus FROM ranked WHERE n=1 ORDER BY IssuerId,Agency"""),
                          'records':query(cn,latest+'SELECT IssuerId,FinancialRecordId,RecordStatus,PeriodStart,PeriodEnd,AccountingBasisCode FROM latest WHERE latest_n=1'),
                          'metrics':query(cn,latest+"""SELECT r.IssuerId,m.FinancialRecordId,m.MetricCode,m.ValueStatus,m.MissingReason,m.ReviewStatus,m.ReviewNote,
                          o.ObservationId,o.NumericValue,o.UnitCode,o.EntityScopeCode,o.AccountingBasisCode,o.MeasurementBasisCode,o.PeriodStart,o.PeriodEnd
                          FROM latest r JOIN DV.IssuerFinancialMetric m ON m.FinancialRecordId=r.FinancialRecordId
                          LEFT JOIN DV.IssuerMetricObservation o ON o.ObservationId=m.ObservationId WHERE r.latest_n=1""")}
            elif action=='library':
                try:data=library_response(cn,q)
                except ValueError as e:return respond(start,'400 Bad Request',{'error':str(e)})
            elif action=='issuer' and id>0:
                issuer=query(cn,'SELECT IssuerId,LegalName,DisplayName,InstitutionType,MonitoringStatus FROM DV.Issuer WHERE IssuerId=?',id)
                if not issuer:return respond(start,'404 Not Found',{'error':'not_found'})
                data={'issuer':issuer[0],
                  'ratings':query(cn,"SELECT c.RatingCandidateId,c.Agency,c.RatingDate,c.Grade,c.PreviousGrade,c.ChangeDirection,c.Outlook,c.RatingAction,c.ReviewStatus,c.DocumentId,c.EvidenceJson,d.Title FROM DV.IssuerRatingCandidate c JOIN DV.IssuerDocument d ON d.DocumentId=c.DocumentId WHERE c.IssuerId=? AND d.DocumentStatus='active' ORDER BY c.RatingDate DESC,c.Agency,c.RatingCandidateId DESC",id),
                  'records':query(cn,SELECTED+'SELECT FinancialRecordId,RecordStatus,PeriodStart,PeriodEnd,AccountingBasisCode FROM ranked WHERE n=1 AND IssuerId=? ORDER BY PeriodEnd DESC',id),
                  'metrics':query(cn,SELECTED+"SELECT m.FinancialRecordId,m.MetricCode,m.ValueStatus,m.MissingReason,m.ReviewStatus,m.ReviewNote,o.ObservationId,o.NumericValue,o.UnitCode,o.EntityScopeCode,o.AccountingBasisCode,o.MeasurementBasisCode,o.PeriodStart,o.PeriodEnd FROM ranked r JOIN DV.IssuerFinancialMetric m ON m.FinancialRecordId=r.FinancialRecordId LEFT JOIN DV.IssuerMetricObservation o ON o.ObservationId=m.ObservationId WHERE r.n=1 AND r.IssuerId=?",id),
                  'documents':query(cn,"SELECT d.DocumentId,d.Title,d.DocumentType,d.MainPeriodEnd,d.PublishedDate,f.PageCount,f.ByteSize,f.ArchiveStatus,CONVERT(varchar(64),f.Sha256,2) Sha256 FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId WHERE d.IssuerId=? AND d.DocumentStatus='active' ORDER BY d.MainPeriodEnd DESC,d.DocumentId",id),
                  'tasks':query(cn,'SELECT TaskId,Stage,Status,OutcomeCode,CreatedAt,FinishedAt FROM DV.IssuerProcessingTask WHERE IssuerId=? AND RootTaskId IS NULL ORDER BY TaskId DESC',id)}
            elif action=='evidence' and id>0:data=query(cn,"SELECT e.DocumentId,e.PdfPage,e.EvidenceRole,e.RawValue,e.RawUnit,e.ColumnHeader,e.Method,e.RawText,d.Title FROM DV.IssuerObservationEvidence e JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId WHERE e.ObservationId=? AND d.DocumentStatus='active' ORDER BY e.EvidenceId",id)
            elif action=='products' and id>0:
                issuer=query(cn,'SELECT IssuerId,LegalName FROM DV.Issuer WHERE IssuerId=?',id)
                if not issuer:return respond(start,'404 Not Found',{'error':'not_found'})
                products=query(cn,'''SELECT TrustId,TrustCode,TrustNameShort,TrustName,ProductSubject,
                    StatusTrust,TrustStatus,TrustStartDate FROM DV.vw_IssuerNplProduct
                    WHERE IssuerId=? ORDER BY TrustId DESC''',id)
                data={'issuer':issuer[0],'products':products,'total':len(products)}
            elif action=='document-info' and id>0:
                rows=query(cn,"SELECT d.DocumentId,d.Title,f.PageCount,f.ByteSize FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId WHERE d.DocumentId=? AND d.DocumentStatus='active' AND f.ArchiveStatus='archived'",id)
                if not rows:return respond(start,'404 Not Found',{'error':'not_found'})
                data=rows[0]
            elif action=='page' and id>0:return page_response(env,start,cn,id,q)
            elif action=='document' and id>0:return document_response(env,start,cn,id)
            else:return respond(start,'400 Bad Request',{'error':'invalid_request'})
            return respond(start,'200 OK',data,head=head)
    except Exception as e:
        print(json.dumps({'event':'issuer_web_error','type':type(e).__name__}),flush=True)
        return respond(start,'503 Service Unavailable',{'error':'service_unavailable'})

if __name__=='__main__':
    from waitress import serve
    serve(app,host='0.0.0.0',port=8080,threads=8)
