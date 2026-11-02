"""Prepare bounded evidence requests and append validated, pending rating candidates."""
import argparse
import gzip
import json

from pilot_runtime import ROOT, ARTIFACTS, CONFIG, ISSUER, atomic_json, encode, sha, guarded_connection
from pilot_v2_prepare import merged_pages
from .rating import request, validate_event, PROMPT_VERSION, parse_response as parse_content


def prepare():
    CONFIG['prompt_version']=PROMPT_VERSION
    atomic_json(ROOT/'run-config.json',CONFIG)
    manifest = json.loads((ROOT / 'prepared/manifest.json').read_text(encoding='utf-8'))
    plan = []
    for doc in manifest:
        pages = merged_pages(doc)
        if any(p.get('coverage_status') == 'ocr_required' or
               (p.get('needs_ocr') and p.get('method') != 'paddleocr') for p in pages.values()):
            raise ValueError('rating_ocr_incomplete')
        # Keep cover/conclusion/history context; long documents require an explicit bounded evidence subset.
        selected = {n:p['text'] for n,p in pages.items() if n <= 6 or any(
            token in p['text'] for token in ('评级结论', '主体信用', '评级日期', '评级时间', '评级结果', '终止评级', '撤销评级'))}
        if len(selected) > 40 or sum(map(len, selected.values())) > 120000:
            raise ValueError('rating_evidence_exceeds_budget')
        payload = request(ISSUER, CONFIG.get('confirmed_aliases', []), selected)
        digest = sha(encode(payload)).hex()
        atomic_json(ARTIFACTS / 'requests' / (digest+'.json'), payload)
        atomic_json(ARTIFACTS / 'inputs' / (digest+'.json'), {'document':doc,'pages':selected})
        target = ROOT / 'input/request-parts' / (digest+'.json.gz')
        target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(gzip.compress(encode(payload)))
        plan.append({'sha256':doc['sha256'],'input_hash':digest,'mode':'rating',
                     'input_bytes':len(encode(payload)),'request_path':'artifacts/requests/'+digest+'.json'})
    atomic_json(ROOT/'input/local-source-manifest.json',manifest)
    atomic_json(ROOT/'input/request-plan-rating.json',plan)


def ingest():
    docs = {d['sha256']:d for d in json.loads((ARTIFACTS/'registered.json').read_text(encoding='utf-8'))}
    registry = json.loads((ARTIFACTS/'request-registry-rating.json').read_text(encoding='utf-8'))
    audit = []
    cn = guarded_connection()
    try:
        for item in registry:
            doc = docs[item['sha256']]
            status = cn.execute('SELECT MonitoringStatus FROM DV.Issuer WITH(UPDLOCK,HOLDLOCK) WHERE IssuerId=?',doc['issuer_id']).fetchval()
            if status != 'active': raise ValueError('issuer_paused')
            attempt = cn.execute('SELECT TOP 1 AttemptId,Status FROM DV.IssuerProcessingAttempt WHERE TaskId=? ORDER BY AttemptNo DESC',item['task_id']).fetchone()
            if attempt[1] not in ('response_saved','succeeded'): raise ValueError('rating_response_not_ready')
            raw = json.loads((ARTIFACTS/'responses'/(str(attempt[0])+'.json')).read_text(encoding='utf-8'))
            if raw['input_hash'] != item['input_hash'] or raw['body']['choices'][0]['finish_reason'] != 'stop':
                raise ValueError('rating_response_input_mismatch')
            body = parse_content(raw['body']['choices'][0]['message']['content'])
            if not isinstance(body.get('events'),list): raise ValueError('rating_schema_mismatch')
            inputs = json.loads((ARTIFACTS/'inputs'/(item['input_hash']+'.json')).read_text(encoding='utf-8'))
            pages = {int(k):v for k,v in inputs['pages'].items()}
            accepted = 0
            for index,event in enumerate(body['events']):
                errors = validate_event(event,pages,ISSUER,CONFIG.get('confirmed_aliases',[]))
                audit.append({'attempt_id':attempt[0],'ordinal':index,'errors':errors})
                if errors:
                    from risk_repository import append_event
                    facts=dict(event) if isinstance(event,dict) else {'raw_event':event}
                    facts.update(event_date=None,extraction_error=';'.join(errors))
                    append_event(cn,doc['issuer_id'],ISSUER,'rating',facts,pages,inputs['document']['source_url'],item['sha256'],doc['document_id'],published=inputs['document'].get('published_date'))
                    continue
                accepted += 1
                if not cn.execute('SELECT RatingCandidateId FROM DV.IssuerRatingCandidate WITH(UPDLOCK,HOLDLOCK) WHERE AttemptId=? AND CandidateOrdinal=?',attempt[0],index).fetchval():
                    cn.execute('''INSERT DV.IssuerRatingCandidate
                    (IssuerId,DocumentId,AttemptId,CandidateOrdinal,Agency,RatingDate,Grade,Outlook,RatingAction,EvidenceJson,PreviousGrade,ChangeDirection)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',doc['issuer_id'],doc['document_id'],attempt[0],index,event['agency'],
                    event['rating_date'],event.get('grade'),event.get('outlook'),event['action'],json.dumps(event['evidence'],ensure_ascii=False),event.get('previous_grade'),event.get('change_direction','未知'))
                if event.get('change_direction') == '下调':
                    from risk_repository import append_event
                    candidate=cn.execute('SELECT RatingCandidateId FROM DV.IssuerRatingCandidate WHERE AttemptId=? AND CandidateOrdinal=?',attempt[0],index).fetchval()
                    facts=dict(event,event_date=event['rating_date'])
                    append_event(cn,doc['issuer_id'],ISSUER,'rating',facts,pages,inputs['document']['source_url'],item['sha256'],doc['document_id'],candidate,inputs['document'].get('published_date'))
            cn.execute("UPDATE DV.IssuerProcessingAttempt SET Status='succeeded' WHERE AttemptId=?",attempt[0])
            cn.execute("UPDATE DV.IssuerProcessingTask SET Status='needs_review',OutcomeCode=?,FinishedAt=SYSUTCDATETIME() WHERE TaskId=?",'rating_candidates_pending' if accepted else 'no_valid_rating_candidate',item['task_id'])
            cn.execute("UPDATE DV.IssuerProcessingTask SET Status='needs_review',OutcomeCode='rating_review_required' WHERE TaskId=?",doc['root_task_id'])
            cn.commit()
        validation=ARTIFACTS/'rating-validation.json'
        if validation.exists():
            previous=validation.read_bytes()
            atomic_json(ARTIFACTS/'rating-validation-history'/(sha(previous).hex()+'.json'),json.loads(previous))
        atomic_json(validation,audit)
    except BaseException:
        cn.rollback(); raise
    finally: cn.close()


if __name__ == '__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['prepare','ingest']);args=parser.parse_args()
    {'prepare':prepare,'ingest':ingest}[args.action]()
