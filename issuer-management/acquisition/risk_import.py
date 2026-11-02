"""Import existing journal checks; search success and parsing backlog are independent."""
import argparse
import hashlib
import json
from pathlib import Path
from pilot_runtime import guarded_connection, atomic_json
from .state import Store
from risk_repository import sync_mappings


def planned_queries(issuer,kind):
    from .worker import KEYWORDS
    short=issuer['name']
    for suffix in ('股份有限公司','有限责任公司','有限公司'):
        if short.endswith(suffix):short=short[:-len(suffix)];break
    return {name+' '+word for name in {issuer['name'],short,*issuer.get('confirmed_aliases',[])} for word in KEYWORDS[kind]}


def grouped_checks(store, inventory):
    groups={};issuers={i['id']:i for i in inventory}
    for row in store.db.execute("SELECT * FROM events WHERE event='risk_check' ORDER BY id"):
        data=json.loads(row['detail']);key=(data['issuer_id'],data['kind'],data['boundary'])
        groups.setdefault(key,{})[data['query']]=(row['id'],data)
    result=[]
    for (iid,kind,boundary),queries in groups.items():
        if iid not in issuers:continue
        expected=planned_queries(issuers[iid],kind)
        latest_id,last=max(queries.values(),key=lambda r:r[0])
        success=all(q in queries and queries[q][1]['status']=='complete' for q in expected)
        errors=sorted({v[1].get('error') or v[1]['status'] for v in queries.values() if v[1]['status']!='complete'})
        if not expected<=queries.keys():errors.append('本批查询尚未完成')
        result.append((str(latest_id)+':aggregate',dict(last,source='chinamoney_'+kind,status='success' if success else 'failed',error=';'.join(errors) or None)))
    return result


def ingest(root, cn):
    store=Store(root)
    try:
        from risk_repository import append_event
        for row in store.db.execute("SELECT detail FROM events WHERE event='authority_material'"):
            material=json.loads(row['detail'])
            append_event(cn,material['issuer_id'],material['issuer_name'],'penalty',material['facts'],
                         {int(k):v for k,v in material['pages'].items()},material['url'],material['digest'],published=material.get('published'))
        inventory_path=Path(root)/'issuers.json'
        inventory=json.loads(inventory_path.read_text()) if inventory_path.exists() else []
        checks=[(str(r['id']),json.loads(r['detail'])) for r in store.db.execute("SELECT * FROM events WHERE event='source_check' ORDER BY id")]
        checks+=grouped_checks(store,inventory)
        for record_id,data in checks:
            source=data['source'];status=data['status'];reason=data.get('error')
            deferred=[r[0] for r in store.db.execute("SELECT DISTINCT error FROM documents WHERE issuer_id=? AND kind=? AND status='processing_failed' AND error LIKE 'quota_deferred:%'",(data['issuer_id'],data.get('kind','penalty')))]
            if deferred:reason=';'.join(([reason] if reason else [])+deferred)
            pending=store.db.execute("SELECT COUNT(*) FROM documents WHERE issuer_id=? AND kind=? AND status IN ('downloaded','prepared','processing_failed','preparation_failed')",(data['issuer_id'],data.get('kind','penalty'))).fetchone()[0]
            key=hashlib.sha256((str(Path(root).resolve())+':'+record_id).encode()).digest()
            if not cn.execute('SELECT RiskCheckLogId FROM DV.RiskCheckLog WHERE CheckKey=?',key).fetchval():
                cn.execute('''INSERT DV.RiskCheckLog(IssuerId,Source,CheckKey,CheckedAt,QueryStart,QueryEnd,Status,FailureReason,ArchiveReference,PendingDocuments)
                  VALUES(?,?,?,?,?,?,?,?,?,?)''',data['issuer_id'],source,key,data['checked_at'][:23],data.get('since'),data.get('until'),status,reason,
                  data.get('archive') or 'acquisition.sqlite/events/'+record_id,pending)
            else:
                cn.execute('UPDATE DV.RiskCheckLog SET PendingDocuments=?,FailureReason=? WHERE CheckKey=?',pending,reason,key)
        # Only advance per-source dates when every planned query in this batch succeeded.
        history_path=Path(root)/'successful-intervals.json'
        history=json.loads(history_path.read_text()) if history_path.exists() else {}
        batches=[json.loads(p.read_text()) for p in sorted((Path(root)/'batches').glob('*.json'))]
        for batch_index,batch in enumerate(batches):
            for key,interval in batch['intervals'].items():
                issuer,kind=key.split(':')
                end=batches[batch_index+1]['started'] if batch_index+1<len(batches) else '9999'
                checks=store.db.execute('''SELECT s.query,s.status FROM searches s JOIN runs r ON r.id=s.run_id
                  WHERE r.started>=? AND r.started<? AND s.issuer_id=? AND s.kind=? ORDER BY r.id''',(batch['started'],end,int(issuer),kind)).fetchall()
                last={r['query']:r['status'] for r in checks}
                inventory_path=Path(root)/'issuers.json'
                inventory=json.loads(inventory_path.read_text()) if inventory_path.exists() else []
                issuer_row=next((i for i in inventory if i['id']==int(issuer)),None)
                if not issuer_row:continue
                expected=planned_queries(issuer_row,kind)
                if expected and all(last.get(q)=='complete' for q in expected):history[key]=max(history.get(key,''),interval['until'])
        cn.commit()
        atomic_json(history_path,history)
    finally:store.db.close()


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--inventory',type=Path)
    a=p.parse_args();cn=guarded_connection()
    try:
        if a.inventory:sync_mappings(cn,json.loads(a.inventory.read_text(encoding='utf-8')))
        ingest(a.root,cn)
    except BaseException:cn.rollback();raise
    finally:cn.close()

if __name__=='__main__':main()
