"""Share request-start pacing across the existing processing containers."""
import os
import time
from pathlib import Path


def wait_for_slot():
    directory = os.environ.get('ISSUER_SITE_STATE')
    if not directory:
        return
    import fcntl
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'gateway-start.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = root / 'gateway-start.txt'
        previous = float(state.read_text()) if state.exists() else 0
        delay = 4.2 - (time.time() - previous)
        if delay > 0:
            time.sleep(delay)
        state.write_text(str(time.time()))
