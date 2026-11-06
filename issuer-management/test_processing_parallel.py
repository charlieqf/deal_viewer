import threading
import time
import unittest
import os
import tempfile
import multiprocessing
import json
from pathlib import Path
from unittest.mock import MagicMock,patch

from acquisition.job_runner import run_jobs


def slot_probe(directory, active, peak, guard):
    from acquisition.job_runner import worker_slot
    with worker_slot(directory):
        with guard:
            active.value+=1
            peak.value=max(peak.value,active.value)
        time.sleep(.15)
        with guard:active.value-=1


def pacing_probe(directory, result):
    from request_pacing import wait_for_slot
    os.environ['ISSUER_SITE_STATE']=directory
    wait_for_slot()
    result.put(time.monotonic())


class ProcessingParallelTests(unittest.TestCase):
    def test_gateway_deferral_leaves_unstarted_documents_queued(self):
        stop=threading.Event()
        jobs=[dict(id=i,path=f'/jobs/1-rating-{i:064x}') for i in range(20)]
        def process(item):
            stop.set()
            return 'processing_failed','quota_deferred:model'
        results=list(run_jobs(jobs,process,4,stop))
        self.assertGreater(len(results),0)
        self.assertLessEqual(len(results),4)

    def test_recovery_respects_paused_and_newly_removed_issuers(self):
        from acquisition.prepare_jobs import prepare_jobs
        from acquisition.state import Store
        for inventory_status,sql_ids in [('paused',[(1,)]),('active',[])]:
            with self.subTest(inventory_status=inventory_status),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);store=Store(root)
                with store.db:
                    store.db.execute('''INSERT INTO documents(issuer_id,kind,source_id,url,title,digest,bytes,pages,first_seen,last_seen,status)
                        VALUES(1,'rating','1','https://example.test/1','example',?,1,1,'now','now','prepared')''',('a'*64,))
                store.db.close()
                cn=MagicMock();cn.execute.return_value=sql_ids
                with patch('acquisition.prepare_jobs.guarded_connection',return_value=cn):
                    prepare_jobs(root,[dict(id=1,monitoring_status=inventory_status)],root/'archive')
                self.assertEqual(json.loads((root/'jobs.json').read_text()),[])

    def test_bounded_parallelism_and_same_document_serialization(self):
        jobs=[dict(id=i,path=f'/jobs/{i//2}-rating-{i//2:064x}') for i in range(12)]
        lock=threading.Lock();active=set();peak=[0];overlap=[]
        def process(item):
            key=item['path']
            with lock:
                if key in active:overlap.append(key)
                active.add(key);peak[0]=max(peak[0],len(active))
            time.sleep(.02)
            with lock:active.remove(key)
            return ('processing_failed','sample_failure') if item['id']==2 else ('pending_review',None)
        results=list(run_jobs(jobs,process,4))
        self.assertEqual(sorted(i['id'] for i,result in results),list(range(12)))
        self.assertFalse(overlap)
        self.assertGreater(peak[0],1)
        self.assertLessEqual(peak[0],4)
        self.assertEqual(sum(result[0]=='processing_failed' for i,result in results),1)

    @unittest.skipUnless(os.name=='posix','Production uses Linux file locks')
    def test_global_slots_across_independent_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            ctx=multiprocessing.get_context('fork')
            active=ctx.Value('i',0);peak=ctx.Value('i',0);guard=ctx.Lock()
            workers=[ctx.Process(target=slot_probe,args=(directory,active,peak,guard)) for _ in range(8)]
            for worker in workers:worker.start()
            for worker in workers:
                worker.join(10);self.assertEqual(worker.exitcode,0)
            self.assertEqual(peak.value,4)
            self.assertEqual(active.value,0)

    @unittest.skipUnless(os.name=='posix','Production uses Linux file locks')
    def test_request_spacing_across_independent_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            ctx=multiprocessing.get_context('fork');result=ctx.Queue()
            workers=[ctx.Process(target=pacing_probe,args=(directory,result)) for _ in range(3)]
            for worker in workers:worker.start()
            for worker in workers:
                worker.join(20);self.assertEqual(worker.exitcode,0)
            starts=sorted(result.get(timeout=1) for _ in workers)
            self.assertTrue(all(b-a>=4.1 for a,b in zip(starts,starts[1:])))


if __name__=='__main__':unittest.main()
