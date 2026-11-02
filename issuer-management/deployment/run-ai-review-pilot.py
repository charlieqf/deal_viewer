"""Host launcher for a bounded, read-only review pilot on R760."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess

BASE = Path('/data/dealviewer-issuer-production')
IMAGE = json.loads((BASE/'acquisition-src/release.json').read_text())['acquisition_image']
OCR = 'sha256:82d620877850d860fee885a74d50e26e3d3cad3b4d59a1b59615d22d9e0fc7ac'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run', required=True)
    parser.add_argument('--issuers', default='14,23,30')
    parser.add_argument('--period', default='2025-06-30')
    args = parser.parse_args()
    import re
    if not re.fullmatch(r'[a-z0-9-]{1,80}', args.run): raise ValueError('Invalid run name')
    root = BASE / 'ai-review' / args.run
    root.mkdir(parents=True, exist_ok=True); os.chown(root, 1200, 1201)
    lock = (BASE/'ai-review/runner.lock').open('a+b')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    source = BASE/'acquisition-src/ai_review.py'
    pinned = root/'ai_review.py'
    if pinned.exists():
        if pinned.read_bytes() != source.read_bytes(): raise RuntimeError('Use a new run for changed code')
    else: shutil.copyfile(source, pinned)
    common = ['docker', 'run', '--rm', '--user', '1200:1201', '--cap-drop', 'ALL',
              '--security-opt', 'no-new-privileges', '-v', str(root)+':'+str(root)]
    runtime = common + ['--network', 'host', '-e', 'ISSUER_PILOT_ROOT='+str(BASE/'acquisition'),
                       '-e', 'DEALVIEWER_SECRETS_FILE=/run/dealviewer/secrets.json',
                       '-v', '/data/dealviewer-crawler/private/secrets.json:/run/dealviewer/secrets.json:ro',
                       '-v', str(BASE/'acquisition')+':'+str(BASE/'acquisition')+':ro',
                       '-v', str(BASE/'archive')+':'+str(BASE/'archive')+':ro',
                       '-v', str(pinned)+':/opt/issuer/ai_review.py:ro']
    if not (root/'snapshot.json').exists():
        subprocess.run(runtime+[IMAGE, '-m', 'ai_review', 'prepare', '--root', str(root),
                                '--issuers', args.issuers, '--period', args.period], check=True, timeout=180)
    subprocess.run(common+['--network', 'none', '--cpus', '2', '--memory', '5g',
                           '-e', 'ISSUER_PILOT_ROOT='+str(root),
                           '-v', str(BASE/'acquisition-src')+':/src:ro',
                           '--entrypoint', '/opt/ocr/bin/python', OCR, '/src/pilot_ocr_v2.py'], check=True, timeout=1800)
    subprocess.run(runtime+['-v', '/data/dealviewer-issuer-pilot/private/gateway.json:/run/review/gateway.json:ro',
                            IMAGE, '-m', 'ai_review', 'review', '--root', str(root)], check=True, timeout=6000)
    lock.close()


if __name__ == '__main__': main()
