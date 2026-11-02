"""One small durable JSON per Beijing Tuesday batch; retries reuse dates and boundary."""
import json
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from risk_rules import window_start
from .state import now


def batch(root, inventory, current=None):
    current=current or datetime.now(timezone(timedelta(hours=8)))
    # A retry before the next Tuesday 04:00 belongs to the preceding batch.
    anchor=current.replace(hour=4,minute=0,second=0,microsecond=0)
    anchor-=timedelta(days=(anchor.weekday()-1)%7)
    if current<anchor: anchor-=timedelta(days=7)
    path=Path(root)/'batches'/(anchor.date().isoformat()+'.json')
    if path.exists(): return json.loads(path.read_text(encoding='utf-8'))
    history=Path(root)/'successful-intervals.json'
    history=json.loads(history.read_text()) if history.exists() else {}
    end=current.date();start=window_start(end)
    intervals={}
    for issuer in inventory:
        for kind in ('rating','default'):
            key=str(issuer['id'])+':'+kind
            previous=history.get(key)
            since=max(start,date.fromisoformat(previous)-timedelta(days=7)) if previous else start
            intervals[key]={'since':since.isoformat(),'until':end.isoformat()}
    value={'id':anchor.date().isoformat(),'started':now(),'intervals':intervals}
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix('.part');temp.write_text(json.dumps(value),encoding='utf-8');temp.replace(path)
    return value


def record_checks(store, run, intervals, boundary):
    """Keep searches at eight columns; completion metadata lives in existing events."""
    for row in store.db.execute('SELECT * FROM searches WHERE run_id=?', (run,)).fetchall():
        key=str(row['issuer_id'])+':'+row['kind']
        with store.db:
            store.db.execute('INSERT INTO events(at,event,detail) VALUES(?,?,?)',
                (now(),'risk_check',json.dumps(dict(row)|intervals[key]|{'boundary':boundary,'checked_at':now()},ensure_ascii=False)))
