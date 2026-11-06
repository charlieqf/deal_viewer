import threading
import time
import unittest
import os
import tempfile
import multiprocessing

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
