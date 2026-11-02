"""Conservative validation of values, rows and explicit context references."""
import re
from pilot_extract import validate_candidate
from pilot_runtime import METRICS
from prepare_pilot import METRIC_TERMS
from financial_text import variants


def compact(text):
    return variants(literal_compact(text))

def literal_compact(text):
    return re.sub(r'\s+','',str(text)).translate(str.maketrans({'（':'(', '）':')','“':'"','”':'"'}))


def partial_profit_scope(code, raw_value, page_text):
    if code!='total_profit':return False
    text=compact(page_text);raw=compact(raw_value)
    if not any(t in text for t in ('构成终止经营','不再包含在本集团业务中')):return False
    return any(raw in m.group(0) for m in re.finditer(r'持续经营活动税前利润[^\d]{0,20}[-(]?\d[\d,.]*',text))

def scope_conflict(code,scope,raw_value,page_text):
    """An explicit scope in the value sentence overrides unrelated page prose."""
    if partial_profit_scope(code,raw_value,page_text):return True
    raw=compact(raw_value).replace('，',',').replace('％','%').rstrip('%')
    text=compact(page_text).replace('，',',').replace('％','%')
    labels='|'.join(map(re.escape,METRIC_TERMS[code]))
    subjects='本公司|本行|母公司' if scope in ('accounting_consolidated','regulatory_consolidated') else '本集团' if scope=='standalone' else None
    if not subjects:return False
    positive='本集团' if scope in ('accounting_consolidated','regulatory_consolidated') else '本公司|本行|母公司'
    if any(raw in m.group(0) for m in re.finditer(r'(?:'+positive+r')(?:的)?(?:'+labels+r')[^。；;]{0,160}',text)):return False
    if any(raw in m.group(0) for m in re.finditer(r'(?:'+subjects+r')(?:的)?(?:'+labels+r')[^。；;]{0,160}',text)):return True
    # A table introduction can declare company scope before a date/header block.
    # Do not borrow unrelated group prose elsewhere on the page as table scope.
    for m in re.finditer(r'(?:下表|表格)[^。；;]{0,50}(?:'+subjects+r')(?:的)?(?:'+labels+r')[^。；;]{0,100}[。；;]',text):
        tail=text[m.end():m.end()+700]
        if raw in tail and not re.search(r'(?:'+positive+r')(?:的)?(?:'+labels+r')',tail):return True
    return False


def profit_row_allowed(code, text):
    text = compact(text)
    if code not in ('net_profit', 'net_profit_attributable_to_parent'):
        return True
    if any(t in text for t in ('扣除非经常性', '扣非', '少数股东', '非控制性', '普通股', '优先股', '永续债', '其他权益工具', '持续经营', '终止经营', '综合收益')):
        return False
    attributable = any(t in text for t in ('归属', '应占', '拥有人'))
    return not attributable if code == 'net_profit' else attributable


def logical_profit_rows(lines):
    result=[]
    for line in lines:
        # IFRS statements distinguish the full-period profit from separately
        # labelled continuing/discontinued operations. Match only the exact row.
        line=re.sub(r'^\s*本期[间間]╱年度利润\s+(?=[-(]?\d)', '净利润 ', line)
        if result and re.fullmatch(r'利润归于[:：]',compact(result[-1])) and re.match(r'\s*[－–-]?本公司股[东東]\s+[-(]?\d',line):
            result[-1]='归属于母公司股东的净利润 '+re.sub(r'^\s*[－–-]?本公司股[东東]\s+','',line)
            continue
        if result and re.fullmatch(r'净利润归属于[:：]?',compact(result[-1])) and re.match(r'\s*母公司股东\s+[-(]?\d',line):
            result[-1]='归属于母公司股东的净利润 '+re.sub(r'^\s*母公司股东\s+','',line)
            continue
        parent_label=r'(?:本行|本银行|银行|母公司|上市公司|公司)(?:股东|所有者)(?:的)?(?:合并)?净利润'
        line=re.sub(r'归属于[:：]\s*(?='+parent_label+r')','归属于',line)
        if result and re.fullmatch(r'归属于[:：]?',compact(result[-1])) and re.match(parent_label,compact(line)):
            result[-1]='归属于'+line.lstrip()
            continue
        if result:
            previous=compact(result[-1])
            previous=re.sub(r'^(?:[一二三四五六七八九十\d]+[.、]|[（(][一二三四五六七八九十\d]+[）)])','',previous)
            terms=METRIC_TERMS['net_profit']+METRIC_TERMS['net_profit_attributable_to_parent']
            if previous and any(t.startswith(previous) and t!=previous and (previous+compact(line)).startswith(t) for t in terms):
                result[-1]=result[-1].rstrip()+line.lstrip()
                continue
        if result and compact(line).startswith(('净利润','合并净利润','的净利润')):
            previous=compact(result[-1])
            previous=re.sub(r'^(?:\d{1,2}[.、]|\(\d{1,2}\))','',previous)
            if not re.search(r'\d',previous) and any(t in previous for t in ('归属','持续经营','终止经营','扣除非经常性','普通股','优先股')):
                result[-1]=result[-1].rstrip()+line.lstrip()
                continue
        result.append(line)
    return result


def ordered_capital_value(text,raw):
    """Bind the total-capital ratio only to its position in an explicit ordered list."""
    pattern=r'(?<!一级)(?<!核心)资本充足率、一级资本充足率、核心一级资本充足率分别为(\d+(?:\.\d+)?)%、(\d+(?:\.\d+)?)%、(\d+(?:\.\d+)?)%'
    return any(m.group(1)==raw for m in re.finditer(pattern,compact(text).replace('％','%')))


def validate(c,doc,pages):
    numeric,unit,value_page=validate_candidate(c,doc,pages)
    code=c['metric_code']; ratio=code in METRICS[1:4]
    if scope_conflict(code,c['entity_scope'],c['raw_value'],value_page['text']):
        raise ValueError('Explicit value sentence contradicts claimed entity scope')
    if ratio and c['accounting_basis']!='not_applicable':
        raise ValueError('Ratios cannot carry an accounting standard')
    if not ratio and c['entity_scope']=='regulatory_consolidated':
        raise ValueError('Accounting amounts cannot use the regulatory consolidation scope')
    if code=='capital_adequacy_ratio' and c['entity_scope']=='accounting_consolidated':
        raise ValueError('Capital ratio requires a regulatory consolidation scope')
    if (c['currency']=='not_applicable') != ratio:
        raise ValueError('Currency semantics do not match the metric')
    raw=compact(c['raw_value']).replace('，',',').replace('％','%').rstrip('%')
    if ',' in raw and not re.fullmatch(r'[-(（]?\d{1,3}(,\d{3})+(\.\d{1,2})?[)）]?',raw):
        raise ValueError('Ambiguous or malformed thousands separators')
    text=compact(value_page['text']).replace('，',',').replace('％','%')
    if raw not in text:
        raise ValueError('Literal raw token absent; automatic punctuation repairs prohibited')
    terms=METRIC_TERMS[code]
    # Check the actual row neighborhood rather than accepting any number on a page.
    lines=logical_profit_rows([variants(line) for line in value_page['text'].splitlines()]); found_row=False; matched_line=None
    label_text=compact('\n'.join(lines)) if code in ('net_profit','net_profit_attributable_to_parent') else text
    if not any(term in label_text for term in terms): raise ValueError('Metric label absent from value page')
    revenue_heading=r'[一二三四五六七八九十、.()0-9]*营业收入'
    revenue_section=code=='operating_revenue' and any(re.fullmatch(revenue_heading,compact(line)) for line in lines)
    for i,line in enumerate(lines):
        line_compact=compact(line)
        if revenue_section and re.fullmatch(revenue_heading,line_compact):continue
        if not profit_row_allowed(code, line_compact):continue
        if code=='capital_adequacy_ratio' and any(t in line_compact for t in ('一级资本充足率','核心资本充足率')):continue
        if code=='total_assets' and '风险加权' in line_compact:continue
        if any(t in line_compact for t in terms):
            # If the metric row already contains numbers, an adjacent row cannot supply its value.
            tail=re.split('|'.join(map(re.escape,terms)),line_compact,maxsplit=1)[-1]
            if code in ('net_profit','net_profit_attributable_to_parent') and not re.search(r'\d',tail):
                numeric_lines=[]
                for following in lines[i+1:i+6]:
                    if re.search(r'[\u4e00-\u9fffA-Za-z]',following):break
                    numeric_lines.append(following)
                neighborhood=compact(line+'\n'+'\n'.join(numeric_lines)).replace('，',',').replace('％','%')
            else:
                neighborhood=compact(line if re.search(r'\d',tail) else '\n'.join(lines[i:i+4])).replace('，',',').replace('％','%')
            if raw in neighborhood: found_row=True; matched_line=line
    if not found_row and code=='operating_revenue':
        # Some statements put the revenue total on an explicit subtotal row.
        # Accept that printed total only within the revenue section; never sum components.
        for i,line in enumerate(lines):
            if not re.fullmatch(revenue_heading,compact(line)):continue
            subtotals=[]
            for candidate in lines[i+1:i+26]:
                current=compact(candidate)
                if any(t in current for t in ('营业支出','营业成本','营业利润','利润总额')):break
                if re.match(r'^(小计|合计)\s*[-(（]?\d',candidate.strip()):subtotals.append(candidate)
            if len(subtotals)==1 and raw in compact(subtotals[0]).replace('，',','):
                found_row=True;matched_line=subtotals[0];break
    if not found_row and not revenue_section:
        # Native PDFs can split a label itself across lines. Follow the logical text only
        # until the next metric label, so adjacent net profit cannot satisfy total profit.
        boundaries=set(t for ts in METRIC_TERMS.values() for t in ts)|{'净利润','营业利润','利息收入','一级资本充足率','核心一级资本充足率'}
        for term in terms:
            for match in re.finditer(re.escape(term),text):
                prefix=text[max(0,match.start()-6):match.start()]
                if code in ('net_profit','net_profit_attributable_to_parent'):
                    # Do not borrow a number across a split profit/attribution row.
                    # Ambiguous split rows remain for manual review.
                    continue
                if code=='capital_adequacy_ratio' and any(x in prefix for x in ('一级','核心')):continue
                if code=='total_assets' and '风险加权' in prefix:continue
                tail=text[match.end():match.end()+180]
                stops=[tail.find(t) for t in boundaries if t not in terms and t in tail]
                if stops:tail=tail[:min(stops)]
                if raw in tail:found_row=True;matched_line='';break
            if found_row:break
    if not found_row and code=='capital_adequacy_ratio' and ordered_capital_value(value_page['text'],raw):
        found_row=True;matched_line=''
    if not found_row: raise ValueError('Number not anchored to metric row; table review required')
    # Common bank statement layout: group current/prior, bank current/prior.
    # Apply only when both the scope header and repeated year headers are explicit.
    header='\n'.join(lines[:18]); year=doc['period_end'][:4]
    # Only actual column labels establish the four-column layout. A narrative
    # "本行及子公司合并数据" also occurs above single-quarter/YTD comparison tables.
    combined_header=any(re.fullmatch(r'(?:项目|附注)?(?:本集团|集团|合并)(?:本行|本银行|银行|母公司|本公司|公司)',compact(line))
                        or re.fullmatch(r'([\u4e00-\u9fff]{1,16})集团\1',compact(line)) for line in lines[:18])
    if not ratio and combined_header and header.count(year)>=2:
        # Preserve token boundaries (do not concatenate adjacent numeric cells).
        tail=re.split('|'.join(map(re.escape,terms)),matched_line,maxsplit=1)[-1]
        tokens=re.findall(r'(?<!\d)[(（-]?\d[\d,，]*(?:\.\d+)?[)）]?',tail)
        if len(tokens)==4:
            expected=tokens[0 if c['entity_scope']=='accounting_consolidated' else 2].replace('，',',')
            if raw!=expected:raise ValueError('Group/bank current column mismatch in four-column table')
    context=set([c['page']]); by_page={p['page']:p for p in pages}
    for key in ('unit_page','scope_page'):
        n=c.get(key)
        if n not in by_page: raise ValueError('Missing supplied '+key)
        context.add(n)
    unit_text=compact(by_page[c['unit_page']]['text'])
    if not ratio:
        unit_term=next((x for x in ('百万元','亿元','万元','千元','元') if x in compact(c['raw_unit'])),None)
        if not unit_term or unit_term not in unit_text: raise ValueError('Amount unit not anchored to cited page')
    scope_text=compact(by_page[c['scope_page']]['text'])
    terms=('集团','合并','本行及其子公司') if c['entity_scope'] in ('accounting_consolidated','regulatory_consolidated') else ('本行','本公司','母公司','银行')
    regulatory_header=c['entity_scope']=='regulatory_consolidated' and re.search(r'(?<![非未不])并表',scope_text)
    if c['entity_scope']!='unknown' and not (any(x in scope_text for x in terms) or regulatory_header):
        raise ValueError('Scope not supported by cited page')
    if c['accounting_basis'] in ('CAS','IFRS'):
        n=c.get('basis_page'); quote=literal_compact(c.get('basis_quote',''))
        if n not in by_page or len(quote)<8 or quote not in literal_compact(by_page[n]['text']):
            raise ValueError('Accounting basis requires a literal evidence quote')
        terms=('企业会计准则','中国会计准则') if c['accounting_basis']=='CAS' else ('国际财务报告准则','国际会计准则')
        if not any(t in variants(quote) for t in terms): raise ValueError('Basis quote does not identify the claimed standard')
        context.add(n)
    return numeric,unit,value_page,context
