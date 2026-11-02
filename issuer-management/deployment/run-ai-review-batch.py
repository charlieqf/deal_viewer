"""R760 host runner: freeze sources, export SQL snapshot, OCR scans, review all groups."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess

BASE=Path('/data/dealviewer-issuer-production')
IMAGE=json.loads((BASE/'acquisition-src/release.json').read_text())['acquisition_image']
OCR='sha256:82d620877850d860fee885a74d50e26e3d3cad3b4d59a1b59615d22d9e0fc7ac'


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--run',required=True);args=parser.parse_args()
    import re
    if not re.fullmatch('[a-z0-9-]{1,80}',args.run):raise ValueError('Invalid run')
    root=BASE/'ai-review'/args.run;root.mkdir(parents=True,exist_ok=True);os.chown(root,1200,1201)
    lock=(BASE/'ai-review/runner.lock').open('a+b');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    for name in ['ai_review.py','ai_review_batch.py','pilot_ocr_v2.py','verify_ai_review_batch.py']:
        source=BASE/'acquisition-src'/name;dest=root/name
        if not dest.exists():shutil.copyfile(source,dest)
        elif dest.read_bytes()!=source.read_bytes():raise RuntimeError('Frozen code differs; new run required')
    common=['docker','run','--rm','--user','1200:1201','--cap-drop','ALL','--security-opt','no-new-privileges','-v',str(root)+':'+str(root)]
    runtime=common+['--network','host','-e','ISSUER_PILOT_ROOT='+str(BASE/'acquisition'),'-e','DEALVIEWER_SECRETS_FILE=/run/dealviewer/secrets.json',
                   '-v','/data/dealviewer-crawler/private/secrets.json:/run/dealviewer/secrets.json:ro',
                   '-v',str(BASE/'acquisition')+':'+str(BASE/'acquisition')+':ro','-v',str(BASE/'archive')+':'+str(BASE/'archive')+':ro',
                   '-v',str(root/'ai_review.py')+':/opt/issuer/ai_review.py:ro','-v',str(root/'ai_review_batch.py')+':/opt/issuer/ai_review_batch.py:ro',
                   '-v',str(root/'verify_ai_review_batch.py')+':/opt/issuer/verify_ai_review_batch.py:ro']
    if not (root/'snapshot.json').exists():subprocess.run(runtime+[IMAGE,'-m','ai_review_batch','export','--root',str(root)],check=True,timeout=1200)
    # Reuse only hash/page-verified OCR, never relabel native text as OCR.
    caches={}
    search=[BASE/'ai-review/pilot-20261029/artifacts/ocr',root/'artifacts/ocr-new',BASE/'acquisition/jobs',BASE/'runs']
    for directory in search:
        pattern='*/*.json' if directory.name in ('ocr','ocr-new') else '*/artifacts/ocr/*/*.json'
        for p in directory.glob(pattern):caches.setdefault((p.parent.name,p.stem),p)
    missing=[]
    for p in (root/'documents').glob('*.json'):
        doc=json.loads(p.read_text());sha=doc['Sha256'].lower();wanted=[]
        for page in doc['pages']:
            key=(sha,str(page['page']));dest=root/'ocr'/sha/(str(page['page'])+'.json')
            if dest.exists():continue
            if key in caches:
                data=json.loads(caches[key].read_text())
                if data.get('sha256')==sha and data.get('page')==page['page']:
                    dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(caches[key],dest);continue
            # Native pages are read directly. Scan pages get explicit OCR.
            if len(page['native'].strip())<80:wanted.append(page['page'])
        if wanted:
            # Only evidence and adjacent pages, not every scan in a 400-page report.
            snap=json.loads((root/'snapshot.json').read_text());evidence={1}
            for r in snap['records']:
                for page in r['evidence_pages'].get(str(doc['DocumentId']),[]):evidence.update([page-1,page,page+1])
            wanted=[p for p in wanted if p in evidence]
        if wanted:
            input_dir=root/'input';input_dir.mkdir(exist_ok=True);file=str(doc['DocumentId'])+'.pdf'
            shutil.copyfile(BASE/'archive'/doc['StorageKey'],input_dir/file)
            missing.append({'pdf':file,'transfer_sha256':sha,'pages':[{'sha256':sha,'page':n,'input_page':n} for n in wanted]})
    if missing:
        (root/'input/ocr-manifest.json').write_text(json.dumps(missing))
        for d,dirs,files in os.walk(root):
            os.chown(d,1200,1201)
            for f in files:os.chown(Path(d)/f,1200,1201)
        from concurrent.futures import ThreadPoolExecutor
        def ocr_shard(shard):
            subprocess.run(common+['--network','none','--cpus','2','--memory','5g','-e','ISSUER_PILOT_ROOT='+str(root),'-e','OCR_OUTPUT=ocr-new',
                                   '-e','OCR_SHARD_COUNT=4','-e','OCR_SHARD_INDEX='+str(shard),
                                   '--entrypoint','/opt/ocr/bin/python',OCR,str(root/'pilot_ocr_v2.py')],check=True,timeout=14400)
        with ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(ocr_shard,range(4)))
        for p in (root/'artifacts/ocr-new').glob('*/*.json'):
            dest=root/'ocr'/p.parent.name/p.name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,dest)
    for d,dirs,files in os.walk(root):
        os.chown(d,1200,1201)
        for f in files:os.chown(Path(d)/f,1200,1201)
    command=runtime+['-v','/data/dealviewer-issuer-pilot/private/gateway.json:/run/review/gateway.json:ro',IMAGE,'-m','ai_review_batch','review','--root',str(root)]
    subprocess.run(command,check=True,timeout=86400)
    subprocess.run(runtime+[IMAGE,'-m','verify_ai_review_batch','--root',str(root)],check=True,timeout=900)


if __name__=='__main__':main()
