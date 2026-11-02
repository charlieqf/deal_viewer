"""Current risk facts, mapping maintenance and the existing append-only change log."""
import hashlib
import json
from datetime import date

from issuer_review import ReviewError, rows, encode
from risk_rules import CATEGORIES, STATUSES, classify, event_key, penalty_total, LIMIT, business_day


def append_event(cn, issuer_id, issuer_name, category, facts, pages, source_url, digest,
                 document_id=None, rating_id=None, published=None):
    status = classify(category, facts, issuer_name, pages)
    if status is None:
        return None
    key = event_key(issuer_id, category, facts, digest)
    evidence = dict(source_url=source_url, sha256=digest, document_id=document_id,
                    evidence=facts.get('evidence', {}), extraction=facts)
    current = rows(cn, 'SELECT * FROM DV.RiskEvent WITH(UPDLOCK,HOLDLOCK) WHERE EventKey=?', key)
    if current:
        old = current[0]
        all_evidence = json.loads(old['EvidenceJson'])
        if evidence not in all_evidence:
            all_evidence.append(evidence)
            cn.execute('UPDATE DV.RiskEvent SET EvidenceJson=?,UpdatedAt=SYSUTCDATETIME() WHERE RiskEventId=?',
                       encode(all_evidence), old['RiskEventId'])
        # A later discovery must not replace a reviewer's correction or original extraction.
        return old['RiskEventId']
    return cn.execute('''INSERT DV.RiskEvent(IssuerId,Category,EventKey,EventDate,PublishedDate,
       SourceUrl,OriginalHash,DocumentId,RatingCandidateId,FieldsJson,RawExtractionJson,EvidenceJson,Status)
       OUTPUT inserted.RiskEventId VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
       issuer_id, category, key, facts.get('event_date'), published, source_url, bytes.fromhex(digest),
       document_id, rating_id, encode(facts), encode(facts), encode([evidence]), status).fetchval()


def log_change(cn, entity, entity_id, before, after, payload):
    cn.execute('''INSERT DV.IssuerChangeLog(EntityType,EntityId,Action,BeforeJson,AfterJson,Reason,ActorId,RequestId,RequestHash)
       VALUES(?,?,'risk_correct',?,?,?,?,?,?)''', entity, entity_id, encode(before), encode(after),
       payload['reason'], payload['reviewer'], payload['request_id'], hashlib.sha256(encode(payload).encode()).digest())


def review(cn, payload):
    import re
    p = dict(payload)
    for field, limit in (('reviewer', 80), ('reason', 2000)):
        if not isinstance(p.get(field), str) or not p[field].strip() or len(p[field]) > limit:
            raise ReviewError('请填写核对人和原因。')
    if not re.fullmatch(r'[A-Za-z0-9_-]{16,100}', str(p.get('request_id', ''))):
        raise ReviewError('请求标识无效。')
    entity = p.get('operation')
    if entity not in ('risk_event', 'product_mapping','risk_add'):
        raise ReviewError('操作类型无效。')
    try: entity_id = int(p['id'])
    except (ValueError, KeyError, TypeError): raise ReviewError('记录标识无效。')
    lock = cn.execute("""DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=?,@LockMode='Exclusive',
      @LockOwner='Transaction',@LockTimeout=10000; SELECT @r""", entity+':'+str(entity_id)).fetchval()
    if lock < 0: raise ReviewError('正在保存，请重试。', '409 Conflict')
    prior = rows(cn, "SELECT RequestHash,AfterJson FROM DV.IssuerChangeLog WHERE EntityType=? AND EntityId=? AND Action='risk_correct' AND RequestId=?",
                 entity, entity_id, p['request_id'])
    if prior:
        if bytes(prior[0]['RequestHash']) != hashlib.sha256(encode(p).encode()).digest():
            raise ReviewError('重复请求内容不一致。', '409 Conflict')
        return {'saved': True, 'replayed': True}
    if entity=='risk_add':
        if p.get('category') not in CATEGORIES or not isinstance(p.get('fields'),dict):raise ReviewError('类别或事实字段无效。')
        document=rows(cn,'''SELECT d.DocumentId,i.LegalName,f.Sha256,f.PageCount FROM DV.IssuerDocument d
           JOIN DV.Issuer i ON i.IssuerId=d.IssuerId JOIN DV.IssuerFile f ON f.FileId=d.FileId
           WHERE d.IssuerId=? AND d.DocumentId=? AND d.DocumentStatus='active' AND f.ArchiveStatus='archived' ''',entity_id,p.get('document_id'))
        if not document:raise ReviewError('请选择该主体已归档原件。')
        doc=document[0];facts=dict(p['fields']);evidence=facts.get('evidence',{}).get('review',{})
        if not evidence.get('quote') or type(evidence.get('page')) is not int or not 1<=evidence['page']<=doc['PageCount']:
            raise ReviewError('请填写原文引文及有效物理页码。')
        # Manual discoveries begin pending; review records facts and applicability in a separate save.
        facts['extraction_error']='manual_entry_requires_confirmation'
        event_id=append_event(cn,entity_id,doc['LegalName'],p['category'],facts,{},'manual:document/'+str(doc['DocumentId']),
                              bytes(doc['Sha256']).hex(),doc['DocumentId'])
        log_change(cn,entity,entity_id,None,{'risk_event_id':event_id},p)
        return {'saved':True,'id':event_id}
    table, id_col = ('DV.RiskEvent', 'RiskEventId') if entity=='risk_event' else ('DV.ProductIssuerMapping', 'ProductId')
    current = rows(cn, 'SELECT * FROM '+table+' WITH(UPDLOCK,HOLDLOCK) WHERE '+id_col+'=?', entity_id)
    if not current: raise ReviewError('记录不存在。', '404 Not Found')
    before = current[0]
    if bytes(before['RowVersion']).hex() != p.get('version'):
        raise ReviewError('记录已更新，请刷新后核对。', '409 Conflict')
    if entity == 'product_mapping':
        iid = p.get('issuer_id')
        try: iid = int(iid) if iid else None
        except (ValueError, TypeError): raise ReviewError('主体标识无效。')
        if iid and not cn.execute('SELECT IssuerId FROM DV.Issuer WHERE IssuerId=?', iid).fetchval():
            raise ReviewError('请先登记发起人。')
        cn.execute("UPDATE DV.ProductIssuerMapping SET IssuerId=?,MappingSource='manual',Basis=?,UpdatedAt=SYSUTCDATETIME() WHERE ProductId=?",
                   iid, p['reason'], entity_id)
    else:
        status = p.get('status')
        if status not in STATUSES: raise ReviewError('事件状态无效。')
        facts = p.get('fields', json.loads(before['FieldsJson']))
        if not isinstance(facts, dict): raise ReviewError('字段格式无效。')
        event_date = p.get('event_date') or None
        try:
            if event_date and date.fromisoformat(event_date)>business_day(): raise ValueError()
        except (ValueError, TypeError): raise ReviewError('事件日期无效。')
        if status=='confirmed':
            if not event_date: raise ReviewError('确认适用需填写事件日期。')
            if before['Category']=='penalty':
                name=cn.execute('SELECT LegalName FROM DV.Issuer WHERE IssuerId=?', before['IssuerId']).fetchval()
                amount,basis=penalty_total(facts,name)
                if basis!='fine' or amount is None or amount<=LIMIT:
                    raise ReviewError('请核清同决定书同法人罚款；确认扣分需严格超过1亿元。')
            if before['Category']=='rating' and facts.get('change_direction')!='下调':
                raise ReviewError('仅明确的本法人主体评级下调适用。')
            if before['Category']=='default' and facts.get('has_default') is not True:
                raise ReviewError('请核清实际违约事实及本法人义务。')
        cn.execute('UPDATE DV.RiskEvent SET EventDate=?,FieldsJson=?,Status=?,Notes=?,UpdatedAt=SYSUTCDATETIME() WHERE RiskEventId=?',
                   event_date, encode(facts), status, p['reason'], entity_id)
    after=rows(cn,'SELECT * FROM '+table+' WHERE '+id_col+'=?',entity_id)[0]
    log_change(cn,entity,entity_id,before,after,p)
    return {'saved': True, 'id': entity_id}


def sync_mappings(cn, inventory):
    from risk_rules import match_products
    products=rows(cn, '''SELECT DISTINCT TrustId ProductId,ProductSubject FROM DV.tbl_view_Products
      WHERE MarketPlaceName=N'银行间债券市场' AND MarketCategoryName=N'信贷资产证券化' ''')
    # Multiple contradictory source rows for one product are unresolved, never arbitrary.
    grouped={}
    for product in products: grouped.setdefault(product['ProductId'],[]).append(product)
    products=[v[0] if len(v)==1 else {'ProductId':k,'ProductSubject':None} for k,v in grouped.items()]
    existing=rows(cn,'SELECT * FROM DV.ProductIssuerMapping WITH(UPDLOCK,HOLDLOCK)')
    for item in match_products(products,inventory,existing):
        if item['MappingSource']=='manual': continue
        cn.execute('''UPDATE DV.ProductIssuerMapping SET IssuerId=?,OriginalProductSubject=?,Basis=?,UpdatedAt=SYSUTCDATETIME()
          WHERE ProductId=? AND MappingSource='auto';
          IF NOT EXISTS(SELECT 1 FROM DV.ProductIssuerMapping WHERE ProductId=?)
          INSERT DV.ProductIssuerMapping(ProductId,IssuerId,OriginalProductSubject,MappingSource,Basis) VALUES(?,?,?,'auto',?)''',
          item['IssuerId'],item['OriginalProductSubject'],item['Basis'],item['ProductId'],item['ProductId'],
          item['ProductId'],item['IssuerId'],item['OriginalProductSubject'],item['Basis'])
    return len(products)


SOURCES = {'qcc': '企查查处罚', 'nfra': '金融监管总局处罚', 'pbc': '人民银行总行处罚',
           'safe': '外汇局总局处罚', 'chinamoney_rating': '中国货币网主体评级',
           'chinamoney_default': '中国货币网违约标题检索', 'international': '国际评级（人工补录）'}


def detail(cn, issuer_id):
    from risk_rules import source_status
    events=rows(cn, 'SELECT * FROM DV.RiskEvent WHERE IssuerId=? ORDER BY DiscoveredAt DESC',issuer_id)
    for event in events:
        event['RowVersion']=bytes(event['RowVersion']).hex()
        event['OriginalHash']=bytes(event['OriginalHash']).hex()
        event.pop('EventKey')
    sources=[]
    for code,label in SOURCES.items():
        latest=rows(cn,'SELECT TOP 1 * FROM DV.RiskCheckLog WHERE IssuerId=? AND Source=? ORDER BY CheckedAt DESC,RiskCheckLogId DESC',issuer_id,code)
        success=rows(cn,"SELECT TOP 1 * FROM DV.RiskCheckLog WHERE IssuerId=? AND Source=? AND Status='success' ORDER BY CheckedAt DESC,RiskCheckLogId DESC",issuer_id,code)
        for record in latest+success: record.pop('CheckKey',None)
        sources.append(dict(code=code,label=label,latest=latest[0] if latest else None,
                            success=success[0] if success else None,**source_status(latest,success[0] if success else None)))
    return json.loads(encode(dict(events=events,sources=sources,summary=rows(cn,'SELECT * FROM DV.vw_IssuerRiskSummary WHERE IssuerId=?',issuer_id)[0])))
