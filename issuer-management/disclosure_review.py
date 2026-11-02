"""Validate a displayed ratio independently from an unconfirmed entity scope."""
import re
from decimal import Decimal

LABELS={'nonperforming_loan_ratio':'不良贷款率','provision_coverage_ratio':'拨备覆盖率'}

def disclosed_ratio(item, doc, page):
    code=item['metric_code']
    if code not in LABELS:raise ValueError('Unsupported disclosure review metric')
    if item['period_end']!=doc['period_end']:raise ValueError('Disclosure period mismatch')
    if item['page']!=page['page']:raise ValueError('Disclosure page mismatch')
    label=LABELS[code]
    if re.search('县域金融业务|境内分行|境外分行|分部主要|零售业务主要',page['text']):
        raise ValueError('Segment disclosure cannot represent the issuer')
    lines=page['text'].splitlines()
    matches=[(i,line) for i,line in enumerate(lines) if re.match(r'^\s*'+label+r'(?:\s|[（(%％])',line)]
    if len(matches)!=1:raise ValueError('Disclosure row ambiguous or not explicitly labelled')
    i,line=matches[0]
    tail=re.sub(r'^\s*'+label+r'\s*(?:[（(]\s*[%％]\s*[)）])?','',line)
    tokens=re.findall(r'(?<!\d)\d+(?:\.\d+)?',tail)
    if not tokens or tokens[0]!=str(item['raw_value']):raise ValueError('Disclosure current-column value mismatch')
    if not Decimal(tokens[0]).is_finite():raise ValueError('Nonfinite ratio')
    preceding=''.join(''.join(lines[:i]).split())
    dates=re.findall(r'(\d{4})年(\d{1,2})月(\d{1,2})日',preceding)
    normalized=['%s-%02d-%02d'%(y,int(m),int(d)) for y,m,d in dates]
    if len(normalized)<2 or normalized[-2]!=item['period_end']:
        raise ValueError('Disclosure current-column date not established')
    if not any(mark in ''.join(lines[:i+1]) for mark in ('%','％')):
        raise ValueError('Disclosure percent unit absent')
    return dict(metric_code=code,raw_value=item['raw_value'],raw_unit='%',
                original_label=label,scope_label='该披露表未明确集团/本行口径，待人工确认',
                column_header=item['period_end'],period_start=item['period_end'],period_end=item['period_end'],
                period_basis='instant',entity_scope='unknown',accounting_basis='not_applicable',
                currency='not_applicable',method='not_specified',page=item['page'],
                unit_page=item['page'],scope_page=item['page'])


def reviewed_candidate(item,doc,pages):
    """An operator-reviewed manifest must bind literal value, period, unit and scope evidence."""
    if 'candidate' not in item:return disclosed_ratio(item,doc,pages[item['page']])
    c=dict(item['candidate']);compact=lambda s:re.sub(r'\s+','',str(s))
    if c['page']!=item['page'] or c['period_end']!=doc['period_end']:
        raise ValueError('Reviewed candidate source/period mismatch')
    proofs=item['proofs']
    for role in ('value','period','unit'):
        proof=proofs[role];quote=compact(proof['quote'])
        if len(quote)<4 or quote not in compact(pages[proof['page']]['text']):
            raise ValueError('Literal review proof absent: '+role)
    if compact(c['raw_value']) not in compact(proofs['value']['quote']):
        raise ValueError('Value not bound to reviewed row')
    if proofs['value']['page']!=c['page'] or proofs['unit']['page']!=c['unit_page']:
        raise ValueError('Review proof page mismatch')
    if not item.get('column_review') or not item.get('scope_review'):
        raise ValueError('Explicit column/scope review required')
    if c['entity_scope']!='unknown':
        proof=proofs['scope'];quote=compact(proof['quote'])
        if proof['page']!=c['scope_page'] or len(quote)<4 or quote not in compact(pages[proof['page']]['text']):
            raise ValueError('Literal scope proof absent')
    return c
