"""Bounded document processing; identical document roots never run together."""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
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


def run_jobs(jobs, process, workers, stop=None):
    groups = OrderedDict()
    for item in jobs:
        name = Path(item['path']).name
        issuer, _, digest = name.split('-', 2)
        groups.setdefault((issuer, digest), []).append(item)

    def group(items):
        results=[]
        for item in items:
            if stop is not None and stop.is_set():break
            results.append((item,process(item)))
        return results

    with ThreadPoolExecutor(max_workers=workers) as pool:
        remaining=iter(groups.values());futures=set()
        def fill():
            while len(futures)<workers and not (stop is not None and stop.is_set()):
                items=next(remaining,None)
                if items is None:break
                futures.add(pool.submit(group,items))
        fill()
        while futures:
            finished,_=wait(futures,return_when=FIRST_COMPLETED)
            for future in finished:
                futures.remove(future)
                yield from future.result()
            fill()
