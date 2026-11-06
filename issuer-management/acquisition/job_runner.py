"""Bounded document processing; identical document roots never run together."""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from contextlib import contextmanager
import time


@contextmanager
def worker_slot(directory):
    """Monthly, weekly and backlog runs share the same four processing slots."""
    import fcntl
    handles = [(Path(directory) / ('processing-slot-%d.lock' % n)).open('a+b') for n in range(4)]
    try:
        while True:
            for handle in handles:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                yield
                return
            time.sleep(0.2)
    finally:
        for handle in handles:
            handle.close()


def run_jobs(jobs, process, workers):
    groups = OrderedDict()
    for item in jobs:
        name = Path(item['path']).name
        issuer, _, digest = name.split('-', 2)
        groups.setdefault((issuer, digest), []).append(item)

    def group(items):
        return [(item, process(item)) for item in items]

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(group, items) for items in groups.values()]
        for future in as_completed(futures):
            yield from future.result()
