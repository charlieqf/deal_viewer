"""Apply the current judging rules to answers saved by completed review runs.

No model is called and nothing is written to business tables. Only former exceptions are
judged again, on the pages their final answers were judged against; former passes stand.
The output directory is accepted by ai_review_writeback.py like any review run.
"""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import shutil
from ai_review import MODELS,save
from ai_review_batch import POLICIES,VERSION,assess,context,domestic_report,negative_controls,stamp

RETRIED=('exception','request_failed')


def judged_pages(root,records,history):
    """Page index per phase, rebuilt as Runner.job built it for this group."""
    first={(f['record_id'],f['metric']):f['status'] for f in history[0]['findings']}
    phases={'initial':records}
    if len(history)>1:
        phases['expanded']=[r for r in records if any(first.get((r['FinancialRecordId'],m['MetricCode'])) in RETRIED for m in r['metrics'])]
    pages={phase:{(p['document_id'],p['page']):p for p in context(root,subset,expanded=phase=='expanded')['pages']}
           for phase,subset in phases.items()}
    return first,pages


def link(source,dest):
    if dest.exists():return
    dest.parent.mkdir(parents=True,exist_ok=True)
    try:os.link(source,dest)
    except OSError:shutil.copyfile(source,dest)


def rejudge(sources,out):
    if (out/'verification.json').exists():raise ValueError('Rejudge output already exists')
    counts=Counter();controls=0;records_out={};origin={}
    for root in sources:
        verification=root/'verification.json';state=json.loads(verification.read_text(encoding='utf-8'))
        if not state.get('review_complete') or state.get('business_writes'):raise ValueError('Source run is incomplete: '+root.name)
        origin[root.name]=hashlib.sha256(verification.read_bytes()).hexdigest()
        snapshot=json.loads((root/'snapshot.json').read_text(encoding='utf-8'))
        records={r['FinancialRecordId']:r for r in snapshot['records']};groups={g['id']:g['record_ids'] for g in snapshot['groups']}
        for path in sorted((root/'results').glob('*.json')):
            data=json.loads(path.read_text(encoding='utf-8'));former=[f for f in data['results'] if f['status']=='exception']
            if not former:continue
            group=[records[i] for i in groups[path.stem]]
            first,pages=judged_pages(root,group,data['history'])
            results=[]
            for f in former:
                record=records[f['record_id']];key=(f['record_id'],f['metric'])
                stored=next(m for m in record['metrics'] if m['MetricCode']==f['metric'])
                phase='expanded' if 'expanded' in pages and first[key] in RETRIED else 'initial'
                allowed={int(d) for d in record['evidence_pages']}
                scoped={k:v for k,v in pages[phase].items() if k[0] in allowed}
                domestic=domestic_report(root,record);answers={m:f['findings'].get(m) or {} for m in MODELS}
                errors={m:assess(answers[m],stored,scoped,record['InstitutionType'],domestic) for m in MODELS}
                status='exception' if any(errors.values()) else 'checks_passed'
                if status=='checks_passed':
                    controls+=negative_controls(answers,stored,scoped,record['InstitutionType'],domestic)
                    records_out[record['FinancialRecordId']]=record
                    for doc in record['evidence_pages']:link(root/'documents'/f'{doc}.json',out/'documents'/f'{doc}.json')
                counts[status]+=1
                results.append(dict(f,status=status,errors=errors,previous_errors=f['errors'],judged_on=phase,
                                    domestic_report=domestic,policies=POLICIES,source_run=root.name))
            save(out/'results'/f'{root.name}-{path.stem}.json',{'version':VERSION,'source_run':root.name,'results':results,'business_writes':False})
    save(out/'snapshot.json',{'version':VERSION,'at':stamp(),'rejudged_from':list(origin),'records':list(records_out.values()),'groups':[]})
    report={'version':VERSION,'policies':POLICIES,'rejudged_from':origin,'counts':dict(counts),'model_calls':0,
            'negative_controls_rejected':controls,'business_writes':False,'review_complete':True}
    save(out/'verification.json',report)
    return report


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--source',type=Path,action='append',required=True,help='Completed review run (read only)')
    p.add_argument('--out',type=Path,required=True);a=p.parse_args()
    print(json.dumps(rejudge(a.source,a.out),ensure_ascii=False))
