"""Host-shared ChinaMoney pacing and pause marker, used by every collection path."""
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def site_slot(kind):
    root=Path(os.environ.get('ISSUER_SITE_STATE', '/data/dealviewer-issuer-production/site-state'))
    root.mkdir(parents=True,exist_ok=True)
    with (root/'chinamoney.lock').open('a+b') as lock:
        if os.name=='nt':
            import msvcrt
            lock.write(b'0');lock.flush();lock.seek(0);msvcrt.locking(lock.fileno(),msvcrt.LK_LOCK,1)
        else:
            import fcntl
            fcntl.flock(lock,fcntl.LOCK_EX)
        if (root/'chinamoney.pause').exists(): raise RuntimeError('chinamoney_paused_by_operator')
        state=root/'chinamoney-times.json'
        times=json.loads(state.read_text()) if state.exists() else {}
        delay=(10 if kind=='download' else 1.5)-(time.time()-times.get(kind,0))
        if delay>0: time.sleep(delay)
        try: yield
        finally:
            times[kind]=time.time()
            tmp=state.with_suffix('.part');tmp.write_text(json.dumps(times));tmp.replace(state)
