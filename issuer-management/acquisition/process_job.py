"""Run one immutable job. OCR is supplied by the dedicated OCR container first."""
import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from pilot_runtime import ROOT, CONFIG, ARTIFACTS, atomic_json, guarded_connection


def main():
    source=Path(__file__).resolve().parents[1]
    def run(script,*args):
        subprocess.run([sys.executable,str(source/script),*args],check=True,timeout=1800)
    if CONFIG['document_kind'] in ('default','penalty'):
        from .risk_pipeline import prepare,ingest
        prepare();run('pilot_runtime.py','register-local')
        run('pilot_register_plan.py','--mode','risk')
        run('pilot_retry_remote.py','--mode','risk','--fresh')
        run('pilot_retry_remote.py','--mode','risk','--recover-saved')
        ingest()
    elif CONFIG['document_kind']=='rating':
        from .rating_pipeline import prepare,ingest
        prepare();run('pilot_runtime.py','register-local')
        run('pilot_register_plan.py','--mode','rating')
        run('pilot_retry_remote.py','--mode','rating','--fresh')
        run('pilot_retry_remote.py','--mode','rating','--recover-saved')
        ingest()
    else:
        run('pilot_v2_prepare.py','--verified-manifest')
        run('pilot_runtime.py','register-local')
        run('pilot_register_plan.py','--mode','v2')
        run('pilot_retry_remote.py','--mode','v2','--fresh')
        run('pilot_retry_remote.py','--mode','v2','--recover-saved')
        run('pilot_build_bundle.py')
        shutil.copyfile(ROOT/'normalized-bundle.json.gz',ROOT/'input/normalized-bundle.json.gz')
        run('pilot_import_bundle.py')
        bundle=json.loads((ROOT/'normalized-bundle.json').read_text(encoding='utf-8'))
        if not any(a['candidates'] for a in bundle['attempts']):
            atomic_json(ARTIFACTS/'processing-result.json',{'status':'needs_evidence_review'})
            cn=guarded_connection()
            try:
                docs=json.loads((ARTIFACTS/'registered.json').read_text(encoding='utf-8'))
                cn.execute("UPDATE DV.IssuerProcessingTask SET Status='needs_review',OutcomeCode='no_valid_financial_candidate' WHERE TaskId=?",docs[0]['root_task_id'])
                cn.commit()
            finally:cn.close()
            return
        run('pilot_v2_drafts.py')
        check=json.loads((ARTIFACTS/'verification.json').read_text(encoding='utf-8'))
        if check['unfinished_children']:raise RuntimeError('unfinished_financial_attempts')


if __name__=='__main__':main()
