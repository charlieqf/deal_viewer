"""Turn archived disclosures into immutable jobs for the existing processing runtime."""
import argparse
import hashlib
import json
import shutil
from datetime import date
from pathlib import Path

from pilot_runtime import atomic_json, guarded_connection, period_from_name
from prepare_pilot import inspect_pdf
from .state import Store, now


def prepare_jobs(root, inventory, production_archive):
    store=Store(root); issuers={i['id']:i for i in inventory}; jobs=[]
    cn=guarded_connection()
    try:
        for row in store.db.execute("SELECT * FROM documents WHERE status IN ('downloaded','preparation_failed') ORDER BY id").fetchall():
            issuer=issuers.get(row['issuer_id'])
            if not issuer or issuer['monitoring_status']!='active': continue
            try:
                if cn.execute('SELECT MonitoringStatus FROM DV.Issuer WHERE IssuerId=?',issuer['id']).fetchval()!='active': continue
                path=Path(root)/'archive'/(row['digest']+'.pdf')
                if hashlib.sha256(path.read_bytes()).hexdigest()!=row['digest']: raise ValueError('archive_hash_mismatch')
                # A known file still acquires a new source record, but does not need another model call.
                existing=cn.execute('''SELECT d.DocumentId FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId
                    WHERE d.IssuerId=? AND f.Sha256=?''',issuer['id'],bytes.fromhex(row['digest'])).fetchval()
                needs_rating_refresh=False
                if existing and row['kind']=='rating':
                    from .rating import PROMPT_VERSION
                    needs_rating_refresh=not cn.execute('''SELECT TOP 1 a.AttemptId FROM DV.IssuerProcessingAttempt a
                        JOIN DV.IssuerProcessingTask t ON t.TaskId=a.TaskId
                        WHERE t.DocumentId=? AND a.PromptVersion=? AND a.Status='succeeded' ''',existing,PROMPT_VERSION).fetchval()
                if existing and row['kind'] not in ('default','penalty') and not needs_rating_refresh:
                    digest=hashlib.sha256(row['url'].encode()).digest()
                    if not cn.execute("SELECT SourceId FROM DV.IssuerDocumentSource WITH(UPDLOCK,HOLDLOCK) WHERE DocumentId=? AND SourceSystem='chinamoney' AND LocatorHash=?",existing,digest).fetchval():
                        cn.execute('''INSERT DV.IssuerDocumentSource(DocumentId,SourceSystem,SourceLocator,LocatorHash,
                            LocatorNormalizationVersion,OriginalFileName,SourcePublisher)
                            VALUES(?,'chinamoney',?,?,'1',?,N'中国货币网')''',existing,row['url'],digest,row['title'])
                    else:
                        cn.execute("UPDATE DV.IssuerDocumentSource SET LastSeenAt=SYSUTCDATETIME() WHERE DocumentId=? AND SourceSystem='chinamoney' AND LocatorHash=?",existing,digest)
                    cn.commit()
                    with store.db: store.db.execute("UPDATE documents SET status='existing_document',error=NULL WHERE id=?",(row['id'],))
                    continue
                if row['kind']=='financial':
                    if any(term in row['title'] for term in ('募集资金使用','存续期评估认证')):
                        with store.db:store.db.execute("UPDATE documents SET status='excluded_document_type',error=NULL WHERE id=?",(row['id'],))
                        continue
                    try:_, period_end, _ = period_from_name(row['title'])
                    except ValueError:
                        with store.db:store.db.execute("UPDATE documents SET status='needs_metadata_review',error='report_period_missing' WHERE id=?",(row['id'],))
                        continue
                    if not date(2024,1,1)<=date.fromisoformat(period_end)<=date.today():
                        with store.db:
                            store.db.execute("UPDATE documents SET status='excluded_period',error=NULL WHERE id=?",(row['id'],))
                            store.db.execute('INSERT INTO events(at,document_id,event,detail) VALUES(?,?,?,?)',
                                (now(),row['id'],'excluded_period',json.dumps({'period_end':period_end})))
                        continue
                job=Path(root)/'jobs'/f"{issuer['id']}-{row['kind']}-{row['digest']}"
                job.mkdir(parents=True,exist_ok=True)
                config=dict(issuer=issuer['name'],display_name=issuer['display_name'],institution_type=issuer['institution_type'],
                    database='PortfolioManagement',pipeline='issuer-production-v1',metrics_version='profit-v2',
                    run_id=job.name,acquisition_version='chinamoney-v1',document_kind=row['kind'],
                    confirmed_aliases=issuer['confirmed_aliases'])
                if row['kind']=='rating':
                    from .rating import PROMPT_VERSION
                    config.update(prompt_version=PROMPT_VERSION,schema_version='issuer-rating-v1')
                atomic_json(job/'run-config.json',config)
                prepared=job/'prepared';prepared.mkdir(exist_ok=True)
                result=inspect_pdf(path,prepared,None)
                result.update(source_name=row['title'].removesuffix('.pdf')+'.pdf',local_path=str(path.resolve()),
                              local_hash_verified=True,source_url=row['url'],published_date=row['published'])
                atomic_json(prepared/'manifest.json',[result])
                atomic_json(job/'input/local-source-manifest.json',[result])
                destination=Path(production_archive)/(row['digest']+'.pdf')
                if not destination.exists():
                    raise RuntimeError('host_archive_publication_required')
                if hashlib.sha256(destination.read_bytes()).hexdigest()!=row['digest']: raise ValueError('production_archive_hash_mismatch')
                atomic_json(job/'input/archive-receipt.json',{'backend':'r760_filesystem','verified_server':True,
                    'files':{row['digest']:{'sha256':row['digest'],'bytes':row['bytes'],'storage_key':destination.name}}})
                # OCR uses the original file and physical page numbers; no lossy preprocessing here.
                source=job/'input/source.pdf';shutil.copyfile(path,source)
                atomic_json(job/'input/ocr-manifest.json',[{'pdf':'source.pdf','transfer_sha256':row['digest'],
                    'pages':[{'sha256':row['digest'],'page':n,'input_page':n} for n in result['ocr_pages']]}])
                jobs.append({'id':row['id'],'path':str(job),'kind':row['kind'],'ocr_pages':len(result['ocr_pages'])})
                with store.db: store.db.execute("UPDATE documents SET status='prepared',error=NULL WHERE id=?",(row['id'],))
            except Exception as error:
                cn.rollback()
                with store.db: store.db.execute("UPDATE documents SET status='preparation_failed',error=? WHERE id=?",(type(error).__name__,row['id']))
        # A restart must recover already prepared jobs too.
        for row in store.db.execute("SELECT * FROM documents WHERE status IN ('prepared','processing_failed')"):
            if row['kind']=='financial' and any(term in row['title'] for term in ('募集资金使用','存续期评估认证')):
                with store.db:store.db.execute("UPDATE documents SET status='excluded_document_type',error=NULL WHERE id=?",(row['id'],))
                continue
            job=Path(root)/'jobs'/f"{row['issuer_id']}-{row['kind']}-{row['digest']}"
            if not any(j['id']==row['id'] for j in jobs):
                manifest=json.loads((job/'prepared/manifest.json').read_text(encoding='utf-8'))
                jobs.append({'id':row['id'],'path':str(job),'kind':row['kind'],'ocr_pages':len(manifest[0]['ocr_pages'])})
        atomic_json(Path(root)/'jobs.json',jobs)
    finally: cn.close();store.db.close()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--issuers',type=Path,required=True)
    p.add_argument('--archive',type=Path,required=True);a=p.parse_args()
    prepare_jobs(a.root,json.loads(a.issuers.read_text(encoding='utf-8')),a.archive)
