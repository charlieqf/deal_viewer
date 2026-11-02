"""Evidence retrieval only: a hit or glossary definition never validates a value/scope."""
import re
from financial_text import search_text
from prepare_pilot import METRIC_TERMS

STATEMENTS = ('合并资产负债表','合并利润表','合并及公司资产负债表','合并及公司利润表',
              '合并及银行资产负债表','合并及银行利润表','合并及本行资产负债表','合并及本行利润表')


def context_pages(pages):
    found=[]
    for n,p in pages.items():
        text=search_text(p['text'])
        glossary=('释义' in text[:350] or '定义' in text[:350]) and any(t in text for t in ('本集团','本行','本公司'))
        declaration=bool(re.search(r'(本集团|本行|本公司).{0,40}(指|包括).{0,100}子公司',text))
        if glossary or declaration:found.append(n)
    return sorted(found)[:4]


def metric_hits(pages,code):
    ranked=[]
    for n,p in pages.items():
        text=search_text(p['text'])
        if not any(t in text for t in METRIC_TERMS[code]):continue
        explicit=any(re.search(r'本集团.{0,90}'+re.escape(t),text) for t in METRIC_TERMS[code])
        statement=any(t in text[:700] for t in STATEMENTS)
        regulatory=code=='capital_adequacy_ratio' and bool(re.search(r'(?<![非未不])并表',text))
        score=8*statement+6*explicit+5*regulatory+3*any(t in text for t in ('百万元','亿元','单位:%','单位：%'))+2*(p['method']=='native_text')
        ranked.append((score,n))
    # Low scoring pages remain usable retrieval candidates; validation is separate.
    return sorted(ranked,key=lambda x:(-x[0],x[1]))


def choose_report(reports,page_sets,code):
    candidates=[]
    for doc in reports:
        hits=metric_hits(page_sets[doc['sha256']],code)
        if hits:
            candidates.append((hits[0][0],doc['document_type']!='audit_report',doc['sha256'],doc,hits))
    if not candidates:return None,[]
    chosen=sorted(candidates,key=lambda x:(-x[0],-x[1],x[2]))[0]
    return chosen[3],chosen[4]


def classify_gap(code,reports,page_sets,errors):
    searched=[{'document_id':d.get('document_id'),'sha256':d['sha256'],
               'hit_pages':[n for _,n in metric_hits(page_sets[d['sha256']],code)]} for d in reports]
    relevant=sorted({e['reason'] for e in errors if e.get('metric') in (None,code)})
    if not any(d['hit_pages'] for d in searched):
        category='material_label_not_found';message='现有材料文字及OCR中未检索到指标标签，需核查或补充材料；不代表未披露。'
    elif any('scope' in e.lower() or 'column mismatch' in e.lower() for e in relevant):
        category='scope_pending';message='已检索到指标相关内容，但集团/本行口径证据不足或存在冲突，待核对。'
    elif any(any(t in e.lower() for t in ('separator','literal raw','anchored to metric row')) for e in relevant):
        category='numeric_evidence_pending';message='已检索到指标相关内容，但数字原文或行列定位未通过校验，需核对原图。'
    elif any('basis' in e.lower() or 'standard' in e.lower() for e in relevant):
        category='basis_pending';message='已检索到指标相关内容，但会计准则依据未通过校验，待核对。'
    else:
        category='evidence_pending';message='已检索到指标相关内容，尚无通过全部校验的集团口径候选，待核对。'
    return {'code':category,'message':message,'searched_documents':searched,'validation_reasons':relevant}
