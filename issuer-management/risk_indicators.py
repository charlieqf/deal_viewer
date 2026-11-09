"""Stable evaluation indicator contract shared by issuer and product callers."""
from preinvestment import item_statuses


INDICATORS = (
    ('pre_a', 'pre', '投前', 'a', 'abs_suspension', '公开信息披露暂停该主体备案新ABS项目的'),
    ('pre_b', 'pre', '投前', 'b', 'servicer_failure', '已发行未结清产品存在存续期管理失职，启动后备催收机构介入情形的'),
    ('pre_c', 'pre', '投前', 'c', 'maturity_unpaid', '存续产品出现到达合同约定的法定到期日仍未全部清偿优先及次级档本金或优先级发行利率、固定资金成本的'),
    ('post_a', 'post', '投后', 'a', 'penalty', '近两年发起人被监管机构处罚，且单次罚款金额超过1亿元'),
    ('post_b', 'post', '投后', 'b', 'rating', '发起人评级下调'),
    ('post_c', 'post', '投后', 'c', 'default', '发起人在债券等公开市场发生违约情形'),
)
POST_SOURCES = {
    'penalty': ('qcc', 'nfra', 'pbc', 'safe'),
    'rating': ('chinamoney_rating',),
    'default': ('chinamoney_default',),
}


def attach_indicators(value):
    """Attach classifications and evidence IDs without scoring or AI inference.

    Keep existing summary/preinvestment_items fields compatible. Display counts
    retain their existing 24-month scope; all historical events remain linked.
    """
    by_category = {}
    for order, (code, stage, stage_label, letter, category, label) in enumerate(INDICATORS, 1):
        by_category[category] = dict(code=code, stage=stage, stage_label=stage_label,
                                    letter=letter, order=order, category=category, label=label)
    for event in value['events']:
        metadata = by_category[event['Category']]
        event.update(IndicatorCode=metadata['code'], IndicatorStage=metadata['stage'],
                     IndicatorLabel=metadata['label'])
    # Use the same status reducer as the existing pre-investment contract.
    post = item_statuses(value['events'], value['sources'], value['summary'],
                         kinds=tuple(POST_SOURCES), source_map=POST_SOURCES,
                         label_map={k: by_category[k]['label'] for k in POST_SOURCES},
                         deduction_fields=('PenaltyDeduction', 'RatingDeduction', 'DefaultDeduction'))
    items = {**value['preinvestment_items'], **post}
    indicators = []
    source_by_code = {source['code']: source for source in value['sources']}
    for category, metadata in by_category.items():
        item = dict(items[category], sources=[dict(check) for check in items[category]['sources']])
        item.pop('suggested_deduction', None)
        relevant = [event for event in value['events'] if event['Category'] == category]
        summary = value['preinvestment_summary'] if metadata['stage'] == 'pre' else value['summary']
        for check in item['sources']:
            check['stale'] = bool(source_by_code.get(check['code'], {}).get('red'))
        indicators.append(dict(item, **{k: v for k, v in metadata.items() if k not in item},
                               event_ids=[event['RiskEventId'] for event in relevant],
                               event_count=len(relevant),
                               excluded_events=sum(event['Status'] not in ('pending', 'confirmed') for event in relevant),
                               historical_events=sum(event['Status'] == 'confirmed' and bool(event.get('EventDate'))
                                   and event['EventDate'] < summary['WindowStart'] for event in relevant),
                               window_start=summary['WindowStart'], business_date=summary['BusinessDate']))
        indicators[-1]['label'] = metadata['label']
    value['indicators'] = indicators
    return value
