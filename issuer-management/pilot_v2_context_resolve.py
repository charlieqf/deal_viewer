"""Resolve bounded layout/context failures without changing numeric characters or calling LLM."""
import gzip
import json
from statistics import median
from pilot_runtime import ROOT,ARTIFACTS,ISSUER,METRICS,atomic_json,encode,sha,guarded_connection
from pilot_extract import parse_content,normalize_method
from pilot_v2_validation import validate,compact
from financial_text import search_text


def reading_order(blocks):
    rows=[]
    for block in sorted(blocks,key=lambda b:((b['box'][1]+b['box'][3])/2,b['box'][0])):
        box=block['box'];center=(box[1]+box[3])/2;height=box[3]-box[1]
        row=rows[-1] if rows else []
        centers=[(b['box'][1]+b['box'][3])/2 for b in row]
        heights=[b['box'][3]-b['box'][1] for b in row]
        if row and abs(center-median(centers))<=.65*min(height,median(heights)):
            row.append(block)
        else:rows.append([block])
    return '\n'.join(' '.join(b['text'] for b in sorted(row,key=lambda b:b['box'][0])) for row in rows)


def context_candidates(model,doc,issuer):
    """Malformed/rejected model members must not stop processing valid siblings."""
    if not isinstance(model,dict) or not isinstance(model.get('candidates'),list):return []
    if search_text(model.get('issuer_name',''))!=search_text(issuer):return []
    if any(model.get(k)!=doc[k] for k in ('period_start','period_end')):return []
    required=('period_start','period_end','period_basis','entity_scope','accounting_basis','currency','method','raw_unit')
    return [(ordinal,c) for ordinal,c in enumerate(model['candidates'],1)
            if isinstance(c,dict) and c.get('metric_code') in METRICS
            and all(isinstance(c.get(k),str) for k in required)
            and 'raw_value' in c and type(c.get('page')) is int]


def main():
    data=json.loads((ARTIFACTS/'sql-readback.json').read_text())
    periods={r['FinancialRecordId']:r['PeriodEnd'] for r in data['records']}
    gaps={(periods[m['FinancialRecordId']],m['MetricCode']) for m in data['metrics'] if m['ValueStatus']!='reported'}
    bundle=json.loads((ROOT/'normalized-bundle.json').read_text());docs={d['sha256']:d for d in json.loads((ARTIFACTS/'registered.json').read_text())}
    cn=guarded_connection();resolved=[]
    for file in sorted((ARTIFACTS/'responses').glob('*.json'),key=lambda p:int(p.stem),reverse=True):
        if not gaps:break
        raw=json.loads(file.read_text())
        if raw.get('status_code')!=200 or not raw.get('body',{}).get('choices'):continue
        if raw['body']['choices'][0].get('finish_reason')!='stop':continue
        info=json.loads((ARTIFACTS/'inputs'/(raw['input_hash']+'.json')).read_text())
        doc=docs[raw['sha256']]
        if not any(end==doc['period_end'] for end,code in gaps):continue
        try:model=parse_content(raw['body']['choices'][0]['message']['content'])
        except (ValueError,KeyError):continue
        for ordinal,candidate in context_candidates(model,doc,ISSUER):
            gap=(candidate['period_end'],candidate['metric_code'])
            if gap not in gaps or candidate['entity_scope']!='accounting_consolidated':continue
            c=dict(candidate);pages=[];regions={};changes=[]
            for p in info['pages']:
                p=dict(p)
                if p['method']=='paddleocr':
                    artifact=ARTIFACTS/p['ocr_artifact'] if p.get('ocr_artifact') else ARTIFACTS/'ocr'/doc['sha256']/(str(p['page'])+'.json')
                    refined=ARTIFACTS/'ocr-refined'/doc['sha256']/(str(p['page'])+'.json')
                    if refined.exists() and refined!=artifact:
                        newer=json.loads(refined.read_text())
                        expected=sha(f"{doc['sha256']}:{p['page']}:{newer.get('engine','')}".encode()).hex()
                        if newer.get('sha256')==doc['sha256'] and newer.get('page')==p['page'] and newer.get('cache_key')==expected:
                            changes.append({'field':'ocr_artifact','page':p['page'],'old':str(artifact.relative_to(ARTIFACTS)),'new':str(refined.relative_to(ARTIFACTS)),'rule':'same_pdf_page_reocr_with_provenance'})
                            artifact=refined;p['ocr_artifact']=str(refined.relative_to(ARTIFACTS))
                    ocr=json.loads(artifact.read_text());text=reading_order(ocr['blocks'])
                    p.update(text=text,layout_version='bbox-height-rows-v3')
                    regions[p['page']]={k:ocr[k] for k in ['width','height','original_width','original_height','rotation_ccw','blocks'] if k in ocr}
                pages.append(p)
            value_page=next((p for p in pages if p['page']==c['page']),None)
            if value_page is None:continue
            unit_page=next((p for p in pages if p['page']==c.get('unit_page')),None)
            term=next((t for t in ['百万元','亿元','万元','千元','元'] if t in c['raw_unit']),None)
            if term and (unit_page is None or term not in compact(unit_page['text'])):
                table=next((t for t in ['资产负债表','利润表','现金流量表'] if t in compact(value_page['text'][:300])),None)
                options=[p for p in pages if abs(p['page']-c['page'])==1 and table and table in compact(p['text'][:300]) and '续' in compact(p['text'][:300]) and term in compact(p['text'][:400]) and doc['period_end'][:4] in p['text'][:400]]
                if len(options)==1:
                    changes.append({'field':'unit_page','old':c.get('unit_page'),'new':options[0]['page'],'rule':'same_statement_adjacent_continuation'})
                    c['unit_page']=options[0]['page']
            try:numeric,unit,source,context=validate(c,doc,pages)
            except (ValueError,KeyError,TypeError):continue
            lineage={'source_attempt_id':raw['attempt_id'],'source_candidate_ordinal':ordinal,'source_response_hash':sha(file.read_bytes()).hex(),'layout':'bbox-height-rows-v3','context_changes':changes,'candidate':c,'pages':pages}
            digest=sha(encode(lineage));key=sha(b'context-resolution-v3:'+digest)
            task=cn.execute('SELECT TaskId FROM DV.IssuerProcessingTask WITH(UPDLOCK,HOLDLOCK) WHERE IdempotencyKey=?',key).fetchval()
            input_path=ARTIFACTS/'context-inputs'/(digest.hex()+'.json');atomic_json(input_path,lineage)
            if task:
                attempt=cn.execute('SELECT AttemptId FROM DV.IssuerProcessingAttempt WHERE TaskId=? AND AttemptNo=1',task).fetchval()
                if any(a['attempt_id']==attempt for a in bundle['attempts']):
                    cn.rollback();gaps.remove(gap);continue
            else:
                task=cn.execute("INSERT DV.IssuerProcessingTask(RootTaskId,ParentTaskId,IssuerId,DocumentId,Stage,InputManifestKey,InputHash,IdempotencyKey,PipelineManifestHash,Status,LeaseOwner,LeaseEpoch,LeaseExpiresAt) OUTPUT inserted.TaskId VALUES(?,?,?,?,?,?,?,?,?,'running','context-resolver',1,DATEADD(hour,1,SYSUTCDATETIME()))",doc['root_task_id'],raw['task_id'],doc['issuer_id'],doc['document_id'],'context_resolution',str(input_path.relative_to(ROOT)),digest,key,sha(b'context-resolution-v3')).fetchval()
                attempt=cn.execute("INSERT DV.IssuerProcessingAttempt(TaskId,AttemptNo,LeaseEpoch,Engine,SchemaVersion,InputHash,InputStorageKey,Status,InputTokens,OutputTokens,UsageStatus) OUTPUT inserted.AttemptId VALUES(?,1,1,'context_resolver','issuer-metrics-v1',?,?,'response_saved',0,0,'known')",task,digest,str(input_path.relative_to(ROOT))).fetchval()
            result_path=ARTIFACTS/'context-results'/(str(attempt)+'.json')
            atomic_json(result_path,dict(lineage,attempt_id=attempt,numeric_value=str(numeric)))
            evidence=[]
            for n in sorted(context):
                p=next(p for p in pages if p['page']==n);page_key=doc['sha256']+':'+str(n)+':'+sha(encode(p)).hex()[:16]
                bundle['pages'][page_key]=dict(p,region=regions.get(n))
                evidence.append({'page_key':page_key,'role':'value' if n==c['page'] else 'context'})
            result=dict(c,ordinal=1,numeric_value=str(numeric),unit_code=unit,measurement_label=c['method'],measurement_code=normalize_method(c['method']),evidence=evidence)
            entry={'attempt_id':attempt,'task_id':task,'lease_epoch':1,'input_hash':digest.hex(),'sha256':doc['sha256'],'result_hash':sha(result_path.read_bytes()).hex(),'result_storage_key':str(result_path.relative_to(ROOT)),'request_id':None,'usage':{'prompt_tokens':0,'completion_tokens':0},'elapsed_seconds':0,'metadata':{'source_attempt_id':raw['attempt_id'],'rules':lineage['layout'],'context_changes':changes},'candidates':[result],'validation_errors':[],'engine':'context_resolver','created_by':'context-resolver'}
            bundle['attempts'].append(entry);cn.commit();gaps.remove(gap);resolved.append({'period':gap[0],'metric':gap[1],'attempt_id':attempt,'source_attempt':raw['attempt_id'],'changes':changes})
    cn.close();atomic_json(ROOT/'normalized-bundle.json',bundle)
    compressed=gzip.compress(encode(bundle))
    (ROOT/'normalized-bundle.json.gz').write_bytes(compressed)
    (ROOT/'input/normalized-bundle.json.gz').write_bytes(compressed)
    atomic_json(ARTIFACTS/'context-resolution.json',{'resolved':resolved,'remaining':sorted(gaps),'new_model_calls':0})
    print(json.dumps({'resolved':resolved,'remaining':sorted(gaps),'new_model_calls':0}),flush=True)


if __name__=='__main__':main()
