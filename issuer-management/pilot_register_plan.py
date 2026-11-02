"""Register local coordinator attempts in SQL before any external model request."""
import argparse
import json

from pilot_runtime import ROOT, ARTIFACTS, PIPELINE, CONFIG, sha, atomic_json, guarded_connection
from request_budget import check_budget


def main(mode):
    docs={x['sha256']:x for x in json.loads((ARTIFACTS/'registered.json').read_text(encoding='utf-8'))}
    plan=json.loads((ROOT/'input'/('request-plan-'+mode+'.json')).read_text(encoding='utf-8'))
    cn=guarded_connection();registry=[]
    cn.execute("DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource='issuer-pilot-gateway-budget',@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=10000; IF @r<0 THROW 51000,'Budget lock unavailable',1;")
    for item in plan:
        doc=docs[item['sha256']];digest=bytes.fromhex(item['input_hash']);key=sha(PIPELINE.encode()+str(doc['root_task_id']).encode()+digest) if CONFIG else sha(b'local-ai:'+digest)
        task=cn.execute('SELECT TaskId,Status,LeaseEpoch FROM DV.IssuerProcessingTask WHERE IdempotencyKey=?',key).fetchone()
        if task:
            attempt=cn.execute('SELECT TOP 1 AttemptId FROM DV.IssuerProcessingAttempt WHERE TaskId=? ORDER BY AttemptNo DESC',task[0]).fetchval()
            if attempt:
                registry.append(dict(item,task_id=task[0],attempt_id=attempt,lease_epoch=task[2]));continue
        task_id=cn.execute("INSERT DV.IssuerProcessingTask(RootTaskId,ParentTaskId,IssuerId,DocumentId,Stage,InputManifestKey,InputHash,IdempotencyKey,PipelineManifestHash,Status,LeaseOwner,LeaseEpoch,LeaseExpiresAt) OUTPUT inserted.TaskId VALUES(?,?,?,?,?,?,?,?,?,'running','spdb-pilot-local',1,DATEADD(hour,4,SYSUTCDATETIME()))",doc['root_task_id'],doc['root_task_id'],doc['issuer_id'],doc['document_id'],'ai_chunk','operator-local:'+item['request_path'],digest,key,sha(PIPELINE.encode())).fetchval()
        count=cn.execute("SELECT COUNT(*) FROM DV.IssuerProcessingAttempt a JOIN DV.IssuerProcessingTask t ON t.TaskId=a.TaskId WHERE t.RootTaskId=? AND a.Engine='gateway'",doc['root_task_id']).fetchval()
        daily=cn.execute("SELECT COUNT(*) FROM DV.IssuerProcessingAttempt WHERE Engine='gateway' AND StartedAt>=CONVERT(date,SYSUTCDATETIME())").fetchval()
        check_budget(count,daily,CONFIG)
        attempt_id=cn.execute("INSERT DV.IssuerProcessingAttempt(TaskId,AttemptNo,LeaseEpoch,Engine,Model,AccountScope,PromptVersion,SchemaVersion,InputHash,InputStorageKey,ReservedTokens,Status) OUTPUT inserted.AttemptId VALUES(?,1,1,'gateway','goldencode','dealviewer-dedicated',?,?,?,?,?,'reserved')",task_id,CONFIG.get('prompt_version','issuer-financial-v2' if CONFIG else 'spdb-financial-v1'),CONFIG.get('schema_version','issuer-metrics-v1'),digest,item['request_path'],item['input_bytes']+12000).fetchval()
        registry.append(dict(item,task_id=task_id,attempt_id=attempt_id,lease_epoch=1))
    cn.commit();cn.close()
    atomic_json(ARTIFACTS/('request-registry-'+mode+'.json'),registry)
    print(json.dumps({'event':'requests_registered','count':len(registry),'mode':mode}),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--mode',default='native');main(p.parse_args().mode)
