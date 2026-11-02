"""Require explicit byte-level evidence before excluding an unreadable source."""
import json

def quarantined(doc,root):
    if doc['page_count']>0:return False
    path=root/'input/source-quarantine.json'
    if not path.exists():raise ValueError('Unreadable PDF requires a recorded quarantine decision')
    entries=json.loads(path.read_text(encoding='utf-8'))['files']
    matches=[e for e in entries if e['sha256']==doc['sha256'] and e['source_name']==doc['source_name']]
    if len(matches)!=1:raise ValueError('Missing or ambiguous source quarantine evidence')
    e=matches[0]
    if not (e['reason_code']=='truncated_pdf' and e['actual_bytes']==doc['bytes'] and e['declared_bytes']>e['actual_bytes']
            and e.get('source_retained') is True and len(e.get('independent_parser_failures',[]))>=2):
        raise ValueError('Insufficient evidence for source quarantine')
    return True
