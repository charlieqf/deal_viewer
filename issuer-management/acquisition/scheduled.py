"""Host-side orchestrator. Browser containers never receive SQL or Gateway credentials."""
import argparse
import json
import hashlib
import os
import re
import shutil
from pathlib import Path
import subprocess
import sys

from .state import Store, now

RELEASE=Path(__file__).resolve().parents[1]/'release.json'
IMAGE=json.loads(RELEASE.read_text())['acquisition_image'] if RELEASE.exists() else 'dealviewer-issuer-acquisition:20261027-v5'
OCR='sha256:82d620877850d860fee885a74d50e26e3d3cad3b4d59a1b59615d22d9e0fc7ac'
PRODUCTION=Path('/data/dealviewer-issuer-production')
ROOT=PRODUCTION/'acquisition'


def main():
    global ROOT
    parser=argparse.ArgumentParser();parser.add_argument('--skip-collect',action='store_true')
    parser.add_argument('--limit-jobs',type=int,default=10)
    parser.add_argument('--collect-only',action='store_true')
    parser.add_argument('--kind',choices=['financial','risk'],default='financial')
    args=parser.parse_args()
    print(json.dumps({'event':'issuer_schedule_started','kind':args.kind,'image':IMAGE,
                      'commit':json.loads(RELEASE.read_text()).get('commit') if RELEASE.exists() else 'development'}),flush=True)
    if args.kind=='risk':ROOT=PRODUCTION/'risk'
    ROOT.mkdir(parents=True,exist_ok=True);os.chown(ROOT,1200,1201)
    inventory=ROOT/('issuers-processing.json' if args.skip_collect else 'issuers.json')
    journal=Store(ROOT)
    # Separate orchestration lock: the collector owns worker.lock inside its container.
    import fcntl
    lock=(ROOT/('processing.lock' if args.skip_collect else 'schedule.lock')).open('a+b')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    processing_lock=None
    base=['docker','run','--rm','--init','--user','1200:1201','--cap-drop','ALL',
          '--security-opt','no-new-privileges','--cpus','2','--memory','3g','--shm-size','512m',
          '-v',str(ROOT)+':'+str(ROOT)]
    site=PRODUCTION/'site-state';site.mkdir(exist_ok=True);os.chown(site,1200,1201)
    base+=['-v',str(site)+':'+str(site),'-e','ISSUER_SITE_STATE='+str(site)]
    def run(module,*arguments,job=None,sql=False,timeout=7200):
        name='dealviewer-issuer-auto-'+module.rsplit('.',1)[-1].replace('_','-')+('-risk' if args.kind=='risk' else '')+('-processing' if args.skip_collect else '')
        cmd=list(base)+['--name',name]
        proxy_file=PRODUCTION/'private/acquisition-network.json'
        if module in ('acquisition.worker','acquisition.risk_sources') and proxy_file.exists():
            cmd+=['-e','ISSUER_ACQUISITION_PROXY_FILE=/run/issuer/proxy.json',
                  '-v',str(proxy_file)+':/run/issuer/proxy.json:ro']
        if sql:
            cmd+=['--network','host','-e','DEALVIEWER_SECRETS_FILE=/run/dealviewer/secrets.json',
                  '-v','/data/dealviewer-crawler/private/secrets.json:/run/dealviewer/secrets.json:ro',
                  '-v',str(PRODUCTION/'archive')+':'+str(PRODUCTION/'archive')+':ro']
        if job:
            cmd+=['-e','ISSUER_PILOT_ROOT='+str(job)]
            if module=='acquisition.process_job':
                cmd+=['-v','/data/dealviewer-issuer-pilot/private/gateway.json:'+str(job/'private/gateway.json')+':ro']
        else:
            cmd+=['-e','ISSUER_PILOT_ROOT='+str(ROOT)]
        try:
            return subprocess.run(cmd+[IMAGE,'-m',module,*map(str,arguments)],check=True,timeout=timeout)
        except subprocess.TimeoutExpired:
            subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            raise
    config=ROOT/'run-config.json'
    if not config.exists():config.write_text(json.dumps({'database':'PortfolioManagement','pipeline':'issuer-production-v1'}))
    os.chown(config,1200,1201)
    for file in ROOT.glob('acquisition.sqlite*'):os.chown(file,1200,1201)
    failed=False
    try:
        run('acquisition.inventory','--out',inventory,sql=True)
        if not args.skip_collect:
            batch_args=[]
            if args.kind=='risk':
                from .risk_batch import batch
                value=batch(ROOT,json.loads(inventory.read_text()))
                batch_file=ROOT/'batches'/(value['id']+'.json');os.chown(batch_file,1200,1201)
                batch_args=['--batch-file',batch_file]
                try:run('acquisition.risk_sources','--root',ROOT,'--issuers',inventory,'--batch-file',batch_file,timeout=3600)
                except (subprocess.CalledProcessError,subprocess.TimeoutExpired):failed=True
            try:run('acquisition.worker','--root',ROOT,'--issuers',inventory,'--resume','--kind',args.kind,*batch_args,timeout=21600)
            except (subprocess.CalledProcessError,subprocess.TimeoutExpired):
                failed=True
                with journal.db:
                    journal.db.execute("UPDATE runs SET status='interrupted',finished=? WHERE status='running'",(now(),))
                    journal.db.execute("UPDATE searches SET status='interrupted',error='worker_stopped' WHERE status='running'")
        if args.collect_only:
            if failed:raise SystemExit(1)
            return
        if not args.skip_collect:
            processing_lock=(ROOT/'processing.lock').open('a+b')
            try:fcntl.flock(processing_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                print(json.dumps({'processing':'already_running','collection_failed':failed}))
                if failed:raise SystemExit(1)
                return
        # The original archive is owned by the host. Browser/SQL containers cannot write it.
        active={r['id'] for r in json.loads(inventory.read_text()) if r['monitoring_status']=='active'}
        for row in journal.db.execute("SELECT issuer_id,digest FROM documents WHERE status IN ('downloaded','preparation_failed')"):
            if row['issuer_id'] not in active:continue
            digest=row['digest']
            if not re.fullmatch('[0-9a-f]{64}',digest):raise ValueError('invalid_archive_digest')
            source=ROOT/'archive'/(digest+'.pdf');target=PRODUCTION/'archive'/(digest+'.pdf')
            if hashlib.sha256(source.read_bytes()).hexdigest()!=digest:raise ValueError('source_archive_hash_mismatch')
            if not target.exists():
                temp=target.with_suffix('.acquisition-part');shutil.copyfile(source,temp)
                os.chmod(temp,0o640);os.chown(temp,1200,1201);temp.replace(target)
            if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:raise ValueError('production_archive_hash_mismatch')
        run('acquisition.prepare_jobs','--root',ROOT,'--issuers',inventory,'--archive',PRODUCTION/'archive',sql=True)
        jobs=json.loads((ROOT/'jobs.json').read_text())
        jobs.sort(key=lambda item:item['kind']!='rating')
        if journal.db.execute("SELECT COUNT(*) FROM documents WHERE status='preparation_failed'").fetchone()[0]:
            failed=True
        for item in jobs[:args.limit_jobs]:
            job=Path(item['path']);(job/'private').mkdir(exist_ok=True);os.chown(job/'private',1200,1201)
            try:
                if item['ocr_pages']:
                    subprocess.run(['docker','run','--rm','--network','none','--cpus','2','--memory','5g',
                        '--user','1200:1201','--cap-drop','ALL','--security-opt','no-new-privileges',
                        '-v',str(ROOT)+':'+str(ROOT),'-v',str(PRODUCTION/'acquisition-src')+':/src:ro',
                        '-e','ISSUER_PILOT_ROOT='+str(job),'--entrypoint','/opt/ocr/bin/python',OCR,
                        '/src/pilot_ocr_v2.py'],check=True,timeout=3600)
                run('acquisition.process_job',job=job,sql=True)
                outcome=job/'artifacts/processing-result.json'
                status=json.loads(outcome.read_text()).get('status') if outcome.exists() else 'pending_review'
                if status not in ('pending_review','needs_evidence_review','archive_only','classified'):raise ValueError('invalid_processing_outcome')
                with journal.db:journal.db.execute("UPDATE documents SET status=?,error=NULL WHERE id=?",(status,item['id']))
            except Exception as error:
                failed=True
                reason=type(error).__name__
                outcome=job/'artifacts/processing-result.json'
                if outcome.exists():reason=json.loads(outcome.read_text()).get('error',reason)
                with journal.db:journal.db.execute("UPDATE documents SET status='processing_failed',error=? WHERE id=?",(reason,item['id']))
        print(json.dumps({'processed':min(len(jobs),args.limit_jobs),'remaining':max(0,len(jobs)-args.limit_jobs),'failed':failed}))
        if failed:raise SystemExit(1)
    finally:
        import_failed=False
        if args.kind=='risk':
            try:run('acquisition.risk_import','--root',ROOT,'--inventory',inventory,sql=True)
            except Exception:
                import_failed=True
                print(json.dumps({'risk_import':'failed'}))
        journal.db.close();lock.close()
        if processing_lock:processing_lock.close()
        if import_failed:raise RuntimeError('risk_import_failed')


if __name__=='__main__':main()
