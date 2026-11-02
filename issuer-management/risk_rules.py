"""V2.3 risk facts and display rules. Recommendations never write evaluation scores."""
import calendar
import hashlib
import json
import re
import unicodedata
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

DEFAULT_TERMS = ('违约', '未按期兑付', '未能按期', '兑付风险', '展期')
STATUSES = ('pending', 'confirmed', 'excluded', 'below_threshold', 'other_debtor')
CATEGORIES = ('penalty', 'rating', 'default')
LIMIT = Decimal('100000000')


def normalize_name(value):
    return unicodedata.normalize('NFKC', value or '').strip()


def business_day():
    return datetime.now(timezone(timedelta(hours=8))).date()


def window_start(day=None):
    day = day or business_day()
    return day.replace(year=day.year-2, day=min(day.day, calendar.monthrange(day.year-2, day.month)[1]))


def default_title_selected(title, filename=''):
    return any(word in (title or '') or word in (filename or '') for word in DEFAULT_TERMS)


def yuan(value, unit='元'):
    if isinstance(value, bool):
        raise ValueError('invalid_amount')
    text = unicodedata.normalize('NFKC', str(value)).replace(',', '').strip()
    match = re.fullmatch(r'(\d+(?:\.\d+)?)\s*(亿元|万元|元)?', text)
    if not match:
        raise ValueError('invalid_amount')
    factor = {'元': 1, '万元': 10000, '亿元': 100000000}.get(match[2] or unit)
    if factor is None:
        raise ValueError('invalid_unit')
    return Decimal(match[1])*factor


def quote_valid(item, pages, value=None):
    if not isinstance(item, dict) or type(item.get('page')) is not int:
        return False
    quote = item.get('quote')
    compact = lambda s: re.sub(r'\s+', '', unicodedata.normalize('NFKC', str(s)))
    return (isinstance(quote, str) and len(compact(quote)) >= 2
            and compact(quote) in compact(pages.get(item['page'], ''))
            and (value is None or compact(value) in compact(quote)))


def penalty_total(facts, issuer):
    """Only a declared complete, reliable set for ONE decision and legal entity.

    Prefer the stated fine total over its components; never sum both. People,
    branches and confiscations are intentionally separate from legal fines.
    """
    if facts.get('amounts_complete') is not True or facts.get('amounts_reliable') is not True:
        return None, None
    if normalize_name(facts.get('party')) != normalize_name(issuer) or facts.get('party_type') != 'legal_entity':
        return None, None
    try:
        if facts.get('fine_total') is not None:
            return yuan(facts['fine_total'], facts.get('unit', '元')), 'fine'
        parts = facts.get('amounts', [])
        eligible = [p for p in parts if p.get('kind') == 'fine' and p.get('party_type') == 'legal_entity'
                    and normalize_name(p.get('party')) == normalize_name(issuer)
                    and p.get('decision_number') == facts.get('decision_number')]
        if eligible and facts.get('decision_number'):
            seen = {}
            for part in eligible:
                # A stable original item identifier is necessary to remove repeated totals/details.
                key = part.get('item_id')
                if not key:
                    return None, None
                amount = yuan(part['amount'], part.get('unit', '元'))
                if key in seen and seen[key] != amount:
                    return None, None
                seen[key] = amount
            return sum(seen.values(), Decimal(0)), 'fine'
        if facts.get('fine_confiscation_total') is not None:
            return yuan(facts['fine_confiscation_total'], facts.get('unit', '元')), 'upper_bound'
    except (ValueError, KeyError, TypeError, InvalidOperation):
        return None, None
    return None, None


def classify(category, facts, issuer, pages):
    """None means archive-only; it is deliberately not a RiskEvent status."""
    if category not in CATEGORIES:
        raise ValueError('invalid_category')
    if facts.get('extraction_error'):
        return 'pending'
    if category == 'penalty':
        amount, basis = penalty_total(facts, issuer)
        if amount is not None and amount <= LIMIT:
            return 'below_threshold'
    elif category == 'default':
        evidence = facts.get('evidence', {})
        if (facts.get('has_default') is False and facts.get('unresolved') is False
                and facts.get('own_obligation_abnormal') is False
                and quote_valid(evidence.get('conclusion'), pages)):
            return None
        debtor = facts.get('debtor')
        if (facts.get('has_default') is True and facts.get('own_obligation_abnormal') is False
                and facts.get('unresolved') is False and debtor
                and normalize_name(debtor) not in {normalize_name(issuer), *map(normalize_name, facts.get('confirmed_aliases', []))}
                and facts.get('issuer_role') in ('lead_underwriter', 'disclosure_agent')
                and quote_valid(evidence.get('debtor'), pages, debtor)
                and quote_valid(evidence.get('issuer_role'), pages, issuer)
                and any(t in evidence['issuer_role']['quote'] for t in ('主承销商', '代为披露', '代发'))):
            return 'other_debtor'
    return 'pending'


def event_key(issuer_id, category, facts, digest):
    if category == 'penalty':
        identity = normalize_name(facts.get('decision_number')) or digest
    elif category == 'rating':
        identity = [normalize_name(facts.get('agency')), facts.get('event_date'), facts.get('grade'), facts.get('change_direction')]
        if not all(identity): identity = digest
    else:
        identity = [normalize_name(facts.get('debtor')), facts.get('debt_id'), facts.get('event_date'), facts.get('action')]
        if not all(identity): identity = digest
    return hashlib.sha256(json.dumps([issuer_id, category, identity], ensure_ascii=False, sort_keys=True).encode()).digest()


def source_status(latest, successful, current=None):
    current = current or datetime.now(timezone.utc)
    if not successful:
        return {'red': True, 'status_label': '尚无成功检查'}
    value = successful['CheckedAt']
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if stamp.tzinfo is None: stamp = stamp.replace(tzinfo=timezone.utc)
    return {'red': current-stamp > timedelta(days=14), 'status_label': '最近成功检查'}


def match_products(products, issuers, existing=()):
    names = {}
    for issuer in issuers:
        for name in [issuer['name'], *issuer.get('confirmed_aliases', [])]:
            key = normalize_name(name)
            if key: names.setdefault(key, set()).add(issuer['id'])
    manual = {r['ProductId']: r for r in existing if r['MappingSource'] == 'manual'}
    result = []
    for product in products:
        pid = product['ProductId']
        if pid in manual:
            result.append(manual[pid]); continue
        matches = names.get(normalize_name(product.get('ProductSubject')), set())
        result.append(dict(ProductId=pid, IssuerId=next(iter(matches)) if len(matches)==1 else None,
                           OriginalProductSubject=product.get('ProductSubject'), MappingSource='auto',
                           Basis='NFKC+trim；名称唯一匹配，角色尚需按原文核对' if len(matches)==1 else '未匹配' if not matches else '多主体匹配'))
    return result
