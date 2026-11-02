"""Retry registered requests through the same Gateway on R760 loopback."""
from concurrent.futures import ThreadPoolExecutor
import argparse
import gzip
import json
import threading
import time

import requests

from pilot_runtime import ROOT,ARTIFACTS,CONFIG,PRODUCTION,atomic_json,sha,encode,guarded_connection
from pilot_extract import parse_content as parse_financial_content
from request_budget import check_budget


def parse_content(content):
    if CONFIG.get('document_kind') in ('rating','default','penalty'):
        from acquisition.rating import parse_response
        return parse_response(content)
    return parse_financial_content(content)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--mode',default='native');parser.add_argument('--fresh',action='store_true');parser.add_argument('--recover-saved',action='store_true');parser.add_argument('--retry-failed',action='store_true');args=parser.parse_args()
    registry=json.loads((ARTIFACTS/('request-registry-'+args.mode+'.json')).read_text(encoding='utf-8'))
    config=json.loads((ROOT/'private/gateway.json').read_text(encoding='utf-8-sig'))
    if args.recover_saved:
        recover_saved(registry);return
    lock=threading.Lock();next_start=[0.0]
    def call(item):
        path=ROOT/'input/request-parts'/(item['input_hash']+'.json.gz')
        if not path.exists():raise FileNotFoundError('Missing registered request part')
        data=gzip.decompress(path.read_bytes())
        if sha(data).hex()!=item['input_hash']:raise ValueError('Request input hash mismatch')
        payload=json.loads(data);input_path=ARTIFACTS/'requests'/(item['input_hash']+'.json');atomic_json(input_path,payload)
        cn=guarded_connection()
        latest=cn.execute('SELECT TOP 1 AttemptId,Status FROM DV.IssuerProcessingAttempt WHERE TaskId=? ORDER BY AttemptNo DESC',item['task_id']).fetchone()
        if args.retry_failed and latest[1]!='failed':
            cn.close();return
        if latest[1] in ('response_saved','succeeded'):
            cn.close();return
        previous_result=ARTIFACTS/'responses'/(str(latest[0])+'.json')
        if CONFIG.get('acquisition_version') and previous_result.exists():
            saved=json.loads(previous_result.read_text(encoding='utf-8'))
            if (saved.get('input_hash')==item['input_hash'] and saved.get('status_code')==200
                    and saved.get('body',{}).get('choices',[{}])[0].get('finish_reason')=='stop'):
                try:parse_content(saved['body']['choices'][0]['message']['content'])
                except (ValueError,KeyError):pass
                else:
                    cn.execute("UPDATE DV.IssuerProcessingAttempt SET Status='response_saved',ResultStorageKey=?,ResultHash=?,FinishedAt=SYSUTCDATETIME() WHERE AttemptId=?",str(previous_result.relative_to(ROOT)),sha(previous_result.read_bytes()),latest[0])
                    cn.commit();cn.close();return
        if latest[1]=='failed' and previous_result.exists():
            failure=json.loads(previous_result.read_text()).get('body',{}).get('error',{})
            if failure.get('code')=='output_length_exceeded':
                # The gateway explicitly forbids identical retries. A later
                # refined/focused request has a new input hash and task.
                cn.execute("UPDATE DV.IssuerProcessingTask SET Status='cancelled',OutcomeCode='output_limit_requires_new_input',FinishedAt=SYSUTCDATETIME() WHERE TaskId=? AND Status='failed'",item['task_id'])
                cn.commit();cn.close()
                print(json.dumps({'event':'identical_retry_skipped','task_id':item['task_id'],'reason':'output_limit_requires_new_input'}),flush=True);return
        retry_failed=latest[1]=='failed'
        fresh=args.fresh
        if CONFIG.get('acquisition_version'):
            fresh=latest[1]=='reserved' and bool(cn.execute('SELECT COUNT(*) FROM DV.IssuerProcessingTask WHERE TaskId=? AND LeaseExpiresAt>SYSUTCDATETIME()',item['task_id']).fetchval())
        if fresh and not retry_failed and (latest[0]!=item['attempt_id'] or latest[1]!='reserved'):
            raise RuntimeError('Fresh request must use an unused reserved attempt')
        if fresh and not retry_failed:
            epoch=item['lease_epoch'];attempt_id=item['attempt_id']
            claimed=cn.execute("UPDATE DV.IssuerProcessingTask SET LeaseOwner='spdb-pilot-r760',LeaseExpiresAt=DATEADD(hour,4,SYSUTCDATETIME()) OUTPUT inserted.TaskId WHERE TaskId=? AND LeaseEpoch=? AND Status='running' AND LeaseExpiresAt>SYSUTCDATETIME()",item['task_id'],epoch).fetchval()
            if not claimed:raise RuntimeError('Fresh request lease is expired or superseded')
            attempt_claimed=cn.execute("UPDATE DV.IssuerProcessingAttempt SET Status='running',InputStorageKey=? OUTPUT inserted.AttemptId WHERE AttemptId=? AND Status='reserved'",str(input_path.relative_to(ROOT)),attempt_id).fetchval()
            if not attempt_claimed:raise RuntimeError('Attempt was already claimed')
        else:
            attempt_id,epoch=reserve_retry(cn,item,input_path,data)
        cn.commit()
        with lock:
            time.sleep(max(0,next_start[0]-time.monotonic()));next_start[0]=time.monotonic()+4.2
        started=time.monotonic();out=ARTIFACTS/'responses'/(str(attempt_id)+'.json')
        print(json.dumps({'event':'request_start','attempt_id':attempt_id,'previous_attempt':None if args.fresh else item['attempt_id'],'transport':'r760-loopback'}),flush=True)
        try:
            r=requests.post('http://127.0.0.1:18787/v1/chat/completions',data=encode(payload),headers={'Authorization':'Bearer '+config['api_key'],'Content-Type':'application/json'},timeout=(10,360))
            body=r.json();result={'attempt_id':attempt_id,'task_id':item['task_id'],'lease_epoch':epoch,'input_hash':item['input_hash'],'sha256':item['sha256'],'status_code':r.status_code,'request_id':r.headers.get('x-request-id'),'elapsed_seconds':round(time.monotonic()-started,3),'body':body,'transport':'r760-loopback'}
            atomic_json(out,result);r.raise_for_status()
            if body['choices'][0]['finish_reason']!='stop':raise ValueError('Incomplete model response')
            parse_content(body['choices'][0]['message']['content'])
            usage=body.get('usage') or {}
            cn.execute("UPDATE DV.IssuerProcessingAttempt SET Status='response_saved',ResultStorageKey=?,ResultHash=?,GatewayRequestId=?,InputTokens=?,OutputTokens=?,UsageStatus=?,FinishedAt=SYSUTCDATETIME() WHERE AttemptId=?",str(out.relative_to(ROOT)),sha(out.read_bytes()),r.headers.get('x-request-id'),usage.get('prompt_tokens'),usage.get('completion_tokens'),'known' if usage else 'unknown',attempt_id)
            cn.commit()
            print(json.dumps({'event':'response_saved','attempt_id':attempt_id,'usage':usage,'seconds':result['elapsed_seconds']}),flush=True)
        except Exception as e:
            cn.rollback();cn.execute("UPDATE DV.IssuerProcessingAttempt SET Status='failed',ErrorCode=?,ResultStorageKey=?,FinishedAt=SYSUTCDATETIME() WHERE AttemptId=?",type(e).__name__,str(out.relative_to(ROOT)) if out.exists() else None,attempt_id)
            output_limit='body' in locals() and isinstance(body,dict) and body.get('error',{}).get('code')=='output_length_exceeded'
            cn.execute("UPDATE DV.IssuerProcessingTask SET Status=?,OutcomeCode=? WHERE TaskId=? AND LeaseEpoch=?",'cancelled' if output_limit else 'failed','output_limit_requires_new_input' if output_limit else type(e).__name__,item['task_id'],epoch);cn.commit()
            print(json.dumps({'event':'request_failed','attempt_id':attempt_id,'error_type':type(e).__name__,'http_status':r.status_code if 'r' in locals() else None}),flush=True)
        finally:cn.close()
    # Dedicated production account permits four concurrent requests; starts stay
    # at least 4.2 s apart (below its 20 requests/minute limit).
    with ThreadPoolExecutor(max_workers=4 if PRODUCTION else 2) as pool:list(pool.map(call,registry))


def recover_saved(registry):
    cn=guarded_connection()
    for item in registry:
        row=cn.execute('SELECT TOP 1 AttemptId,Status,ErrorCode FROM DV.IssuerProcessingAttempt WHERE TaskId=? ORDER BY AttemptNo DESC',item['task_id']).fetchone()
        if row[1]!='failed' or row[2]!='JSONDecodeError':continue
        path=ARTIFACTS/'responses'/(str(row[0])+'.json');raw=json.loads(path.read_text())
        if raw.get('status_code')!=200 or raw['body']['choices'][0]['finish_reason']!='stop':continue
        try:parse_content(raw['body']['choices'][0]['message']['content'])
        except (ValueError,KeyError):continue
        if raw['input_hash']!=item['input_hash']:raise ValueError('Saved response input mismatch')
        claimed=cn.execute("UPDATE DV.IssuerProcessingTask SET Status='running',LeaseOwner='issuer-saved-recovery',LeaseExpiresAt=DATEADD(hour,4,SYSUTCDATETIME()),OutcomeCode=NULL OUTPUT inserted.TaskId WHERE TaskId=? AND LeaseEpoch=? AND Status='failed' AND OutcomeCode='JSONDecodeError'",item['task_id'],raw['lease_epoch']).fetchval()
        if not claimed:raise RuntimeError('Recovery lease was superseded')
        usage=raw['body'].get('usage') or {}
        cn.execute("UPDATE DV.IssuerProcessingAttempt SET Status='response_saved',ErrorCode=NULL,ResultHash=?,InputTokens=?,OutputTokens=?,UsageStatus=?,GatewayRequestId=? WHERE AttemptId=? AND Status='failed'",sha(path.read_bytes()),usage.get('prompt_tokens'),usage.get('completion_tokens'),'known' if usage else 'unknown',raw.get('request_id'),row[0])
        cn.commit(); print(json.dumps({'event':'saved_response_recovered','attempt_id':row[0],'additional_model_calls':0}),flush=True)
    cn.close()


def reserve_retry(cn,item,input_path,data):
        cn.execute("DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource='issuer-pilot-gateway-budget',@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=10000; IF @r<0 THROW 51000,'Budget lock unavailable',1;")
        root=cn.execute('SELECT RootTaskId FROM DV.IssuerProcessingTask WHERE TaskId=?',item['task_id']).fetchval()
        count=cn.execute("SELECT COUNT(*) FROM DV.IssuerProcessingAttempt a JOIN DV.IssuerProcessingTask t ON t.TaskId=a.TaskId WHERE t.RootTaskId=?",root).fetchval()
        daily=cn.execute("SELECT COUNT(*) FROM DV.IssuerProcessingAttempt WHERE Engine='gateway' AND StartedAt>=CONVERT(date,SYSUTCDATETIME())").fetchval()
        check_budget(count,daily,CONFIG)
        epoch=cn.execute("UPDATE DV.IssuerProcessingTask SET LeaseEpoch=LeaseEpoch+1,LeaseOwner='spdb-pilot-r760',LeaseExpiresAt=DATEADD(hour,4,SYSUTCDATETIME()),Status='running' OUTPUT inserted.LeaseEpoch WHERE TaskId=? AND (Status='failed' OR LeaseExpiresAt<SYSUTCDATETIME())",item['task_id']).fetchval()
        if epoch is None:raise RuntimeError('Cannot retry a live task')
        cn.execute("UPDATE DV.IssuerProcessingAttempt SET Status='failed',ErrorCode='client_network_unknown',UsageStatus='unknown',FinishedAt=SYSUTCDATETIME() WHERE AttemptId=? AND Status='reserved'",item['attempt_id'])
        number=cn.execute('SELECT ISNULL(MAX(AttemptNo),0)+1 FROM DV.IssuerProcessingAttempt WHERE TaskId=?',item['task_id']).fetchval()
        if number>3:raise RuntimeError('Retry limit reached')
        attempt_id=cn.execute("INSERT DV.IssuerProcessingAttempt(TaskId,AttemptNo,LeaseEpoch,Engine,Model,AccountScope,PromptVersion,SchemaVersion,InputHash,InputStorageKey,ReservedTokens,Status) OUTPUT inserted.AttemptId VALUES(?,?,?,'gateway','goldencode','dealviewer-dedicated',?,?,?,?,?,'running')",item['task_id'],number,epoch,CONFIG.get('prompt_version','issuer-financial-v2' if CONFIG else 'spdb-financial-v1'),CONFIG.get('schema_version','issuer-metrics-v1'),bytes.fromhex(item['input_hash']),str(input_path.relative_to(ROOT)),len(data)+12000).fetchval()
        return attempt_id,epoch


if __name__=='__main__':main()
