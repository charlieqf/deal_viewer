"""Validate model output against supplied pages and produce a typed import bundle."""
import gzip
import argparse
import json
import re
import sys

from pilot_runtime import ROOT,ARTIFACTS,ISSUER,CONFIG,atomic_json,sha
from pilot_extract import validate_candidate,normalize_method,parse_content
from financial_text import search_text


def complete_response(raw):
    choice=raw['body']['choices'][0]
    if choice.get('finish_reason')!='stop':raise ValueError('Truncated response')
    result=parse_content(choice['message']['content'])
    if not isinstance(result,dict):raise ValueError('Response must be a JSON object')
    return result


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--revalidate',action='store_true');args=parser.parse_args()
    bundle={'schema_version':1,'attempts':[],'pages':{}}
    prior=json.loads((ROOT/'normalized-bundle.json').read_text(encoding='utf-8')) if CONFIG and (ROOT/'normalized-bundle.json').exists() else {'attempts':[],'pages':{}}
    frozen={x['attempt_id']:x for x in prior['attempts']}
    if args.revalidate and prior['attempts']:
        snapshot=ROOT/'normalized-bundle.json';digest=sha(snapshot.read_bytes()).hex()
        archive=ARTIFACTS/'revalidation';archive.mkdir(exist_ok=True)
        (archive/(digest+'.json')).write_bytes(snapshot.read_bytes())
    for entry in prior['attempts']:
        if entry.get('engine')=='context_resolver':
            bundle['attempts'].append(entry)
            for c in entry['candidates']:
                for e in c['evidence']:bundle['pages'][e['page_key']]=prior['pages'][e['page_key']]
    summary=[]
    for file in sorted((ARTIFACTS/'responses').glob('*.json'),key=lambda p:int(p.stem)):
        raw=json.loads(file.read_text(encoding='utf-8'))
        if raw.get('status_code')!=200:continue
        if raw['attempt_id'] in frozen and frozen[raw['attempt_id']]['result_hash']!=sha(file.read_bytes()).hex():
            raise ValueError('Previously validated raw response was modified')
        # Failed/truncated transport responses remain in the raw audit trail,
        # never in an import bundle. Retrying them creates a separate attempt.
        try:parsed=complete_response(raw)
        except (ValueError,KeyError,TypeError,IndexError) as error:
            summary.append({'attempt_id':raw['attempt_id'],'valid_candidates':0,'excluded_from_import':True,'errors':[{'reason':str(error)}]})
            continue
        if raw['attempt_id'] in frozen and not args.revalidate:
            previous=frozen[raw['attempt_id']]
            bundle['attempts'].append(previous)
            for c in previous['candidates']:
                for e in c['evidence']:bundle['pages'][e['page_key']]=prior['pages'][e['page_key']]
            continue
        digest=raw['input_hash'];info=json.loads((ARTIFACTS/'inputs'/(digest+'.json')).read_text(encoding='utf-8'))
        doc,pages=info['document'],info['pages'];errors=[];candidates=[]
        try:
            result=parsed
            if search_text(result['issuer_name'])!=search_text(ISSUER):raise ValueError('Issuer mismatch')
            if result.get('period_start')!=doc['period_start'] or result.get('period_end')!=doc['period_end']:raise ValueError('Report period mismatch')
            for ordinal,c in enumerate(result['candidates'],1):
                try:
                    if CONFIG:
                        from pilot_v2_validation import validate
                        numeric,unit,p,chosen_pages=validate(c,doc,pages)
                    else:
                        numeric,unit,p=validate_candidate(c,doc,pages)
                        chosen_pages={c['page']}
                    page_key=doc['sha256']+':'+str(c['page'])
                    if not CONFIG:
                        if any(x['page']==c['page']-1 for x in pages):chosen_pages.add(c['page']-1)
                        chosen_pages.update(int(x) for x in re.findall(r'第\s*(\d+)\s*页',c.get('evidence_text','')+' '+c.get('notes','')) if any(p['page']==int(x) for p in pages))
                    evidence=[]
                    for num in sorted(chosen_pages):
                        source=next(p for p in pages if p['page']==num);key=doc['sha256']+':'+str(num)+(':'+sha(json.dumps(source,ensure_ascii=False,sort_keys=True).encode()).hex()[:16] if CONFIG else '')
                        region=None
                        if source['method']=='paddleocr':
                            ocr_path=ARTIFACTS/source['ocr_artifact'] if source.get('ocr_artifact') else ARTIFACTS/'ocr'/doc['sha256']/(str(num)+'.json')
                            ocr=json.loads(ocr_path.read_text(encoding='utf-8'))
                            region={k:ocr[k] for k in ['width','height','original_width','original_height','rotation_ccw','blocks'] if k in ocr}
                        bundle['pages'][key]=dict(source,region=region)
                        evidence.append({'page_key':key,'role':'value' if num==c['page'] else 'context'})
                    candidates.append(dict(c,ordinal=ordinal,numeric_value=str(numeric),unit_code=unit,measurement_label=c['method'],measurement_code=normalize_method(c['method']),evidence=evidence))
                except Exception as e:errors.append({'ordinal':ordinal,'metric':c.get('metric_code'),'reason':str(e)})
        except Exception as e:
            result={};errors.append({'reason':str(e)})
        entry={'attempt_id':raw['attempt_id'],'task_id':raw['task_id'],'lease_epoch':raw.get('lease_epoch',1),'input_hash':digest,'sha256':doc['sha256'],'result_hash':sha(file.read_bytes()).hex(),'result_storage_key':str(file.resolve()),'request_id':raw.get('request_id'),'usage':raw['body'].get('usage') or {},'elapsed_seconds':raw.get('elapsed_seconds'),'metadata':{k:v for k,v in result.items() if k!='candidates'},'candidates':candidates,'validation_errors':errors}
        bundle['attempts'].append(entry)
        summary.append({'attempt_id':raw['attempt_id'],'file':doc['source_name'],'valid_candidates':len(candidates),'errors':errors})
    path=ROOT/'normalized-bundle.json';atomic_json(path,bundle)
    (ROOT/'normalized-bundle.json.gz').write_bytes(gzip.compress(path.read_bytes()))
    atomic_json(ROOT/'normalization-summary.json',summary)
    print(json.dumps({'attempts':len(bundle['attempts']),'candidates':sum(len(x['candidates']) for x in bundle['attempts']),'rejected':sum(len(x['validation_errors']) for x in bundle['attempts']),'gzip_bytes':(ROOT/'normalized-bundle.json.gz').stat().st_size,'summary':summary},ensure_ascii=False),flush=True)


if __name__=='__main__':
    sys.stdout.reconfigure(encoding='utf-8');main()
