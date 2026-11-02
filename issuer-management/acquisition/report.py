"""Read-only acquisition progress and stable-source failure export (no signed URLs)."""
import argparse
import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def report(root, output):
    root, output = Path(root), Path(output)
    inventory = json.loads((root / 'issuers.json').read_text(encoding='utf-8'))
    active = {x['id']: x['name'] for x in inventory if x['monitoring_status'] == 'active'}
    db = sqlite3.connect((root / 'acquisition.sqlite').resolve().as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        queries = [dict(r) for r in db.execute('''SELECT s.* FROM searches s
            JOIN (SELECT issuer_id,kind,query,MAX(run_id) run_id FROM searches
                  GROUP BY issuer_id,kind,query) latest
            USING(issuer_id,kind,query,run_id)''')]
        searched = {r['issuer_id'] for r in queries} & active.keys()
        failures = [dict(r) for r in db.execute('''SELECT f.*,d.source_id,d.title,d.published
            FROM download_failures f LEFT JOIN download_failure_details d
            USING(issuer_id,kind,url) ORDER BY f.issuer_id,f.kind,f.url''')]
        result = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'active_issuers': len(active), 'issuers_search_started': len(searched),
            'not_yet_searched': [{'id': k, 'name': v} for k,v in active.items() if k not in searched],
            'documents': [dict(r) for r in db.execute('SELECT kind,status,COUNT(*) count FROM documents GROUP BY kind,status')],
            'unique_pdf_hashes': db.execute('SELECT COUNT(DISTINCT digest) FROM documents').fetchone()[0],
            'latest_run': dict(db.execute('SELECT * FROM runs ORDER BY id DESC LIMIT 1').fetchone() or {}),
            'download_failures': len(failures),
            'note': 'Search started is not completed coverage; document pending_review is not a count of validated rating candidates.',
        }
    finally:
        db.close()
    output.mkdir(parents=True, exist_ok=True)
    (output / 'progress.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    with (output / 'download-failures.csv').open('w',encoding='utf-8-sig',newline='') as file:
        fields=['issuer_id','issuer_name','kind','source_id','title','published','url','error','last_seen']
        writer=csv.DictWriter(file,fieldnames=fields);writer.writeheader()
        for row in failures:
            row['issuer_name']=active.get(row['issuer_id'],'')
            # No spreadsheet formula execution from untrusted source titles.
            writer.writerow({k: ("'"+str(v) if str(v).startswith(('=','+','-','@')) else v) for k,v in row.items()})
    return result


if __name__ == '__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    print(json.dumps(report(args.root,args.output),ensure_ascii=True))
