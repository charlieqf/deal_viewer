import copy
import unittest

from preinvestment import item_statuses, SOURCES
from risk_indicators import attach_indicators, INDICATORS, POST_SOURCES


def fixture(events=()):
    summary = dict(WindowStart='2024-11-09', BusinessDate='2026-11-09',
                   PenaltyDeduction=0, RatingDeduction=0, DefaultDeduction=0,
                   SuspensionDeduction=0, ServicerDeduction=0, MaturityDeduction=0)
    sources = [dict(code=code, label=code, red=False, latest=dict(Status='success',
                   QueryStart=summary['WindowStart'], QueryEnd=summary['BusinessDate'], PendingDocuments=0))
               for codes in {**SOURCES, **POST_SOURCES}.values() for code in codes]
    value = dict(events=copy.deepcopy(list(events)), sources=sources,
                 summary=summary, preinvestment_summary=summary.copy())
    value['preinvestment_items'] = item_statuses(value['events'], sources, summary)
    return value


class IndicatorContractTests(unittest.TestCase):
    def test_six_stable_ordered_indicators_without_scores(self):
        value = attach_indicators(fixture())
        self.assertEqual([x['code'] for x in value['indicators']],
                         ['pre_a', 'pre_b', 'pre_c', 'post_a', 'post_b', 'post_c'])
        for item in value['indicators']:
            self.assertNotIn('suggested_deduction', item)
            self.assertEqual(item['status'], 'not_detected')
            self.assertEqual(item['event_ids'], [])
        self.assertEqual(value['indicators'][3]['label'], '近两年发起人被监管机构处罚，且单次罚款金额超过1亿元')

    def test_events_link_to_exact_indicator_including_exclusions(self):
        events = [dict(RiskEventId=i, Category=definition[4], Status='excluded', EventDate=None)
                  for i, definition in enumerate(INDICATORS, 1)]
        value = attach_indicators(fixture(events))
        for event, item in zip(value['events'], value['indicators']):
            self.assertEqual(event['IndicatorCode'], item['code'])
            self.assertEqual(event['IndicatorLabel'], item['label'])
            self.assertEqual(item['event_ids'], [event['RiskEventId']])
            self.assertEqual(item['excluded_events'], 1)
            self.assertFalse(item['requires_review'])

    def test_window_and_pending_are_independent_of_scores(self):
        events = [dict(RiskEventId=i, Category='rating', Status=status, EventDate=day)
                  for i, (status, day) in enumerate([
                      ('confirmed', '2024-11-09'), ('confirmed', '2026-11-09'),
                      ('confirmed', '2024-11-08'), ('pending', None)], 1)]
        item = attach_indicators(fixture(events))['indicators'][4]
        self.assertEqual(item['confirmed_events'], 2)
        self.assertEqual(item['historical_events'], 1)
        self.assertEqual(item['pending_events'], 1)
        self.assertTrue(item['requires_review'])
        self.assertEqual(item['status'], 'confirmed')
        self.assertEqual(item['event_count'], 4)

    def test_failed_missing_and_backlog_are_not_negative_findings(self):
        for state, expected in [('failed', 'query_failed'), ('not_connected', 'not_connected'),
                                ('not_queried', 'not_queried'), ('success', 'pending_processing')]:
            with self.subTest(state=state):
                value = fixture()
                source = next(x for x in value['sources'] if x['code'] == 'chinamoney_default')
                source['latest'].update(Status=state, PendingDocuments=2)
                item = attach_indicators(value)['indicators'][5]
                self.assertEqual(item['status'], expected)
                self.assertFalse(item['requires_review'])

    def test_sources_and_backlog_do_not_leak_between_categories(self):
        value = fixture()
        next(x for x in value['sources'] if x['code'] == 'qcc')['latest'].update(Status='not_connected', PendingDocuments=99)
        items = attach_indicators(value)['indicators']
        self.assertEqual(items[3]['status'], 'not_connected')
        self.assertEqual(items[4]['status'], 'not_detected')
        self.assertEqual(items[4]['pending_documents'], 0)

    def test_existing_contract_unchanged(self):
        value = fixture()
        old = copy.deepcopy(value)
        attach_indicators(value)
        for key in ('summary', 'preinvestment_summary', 'preinvestment_items'):
            self.assertEqual(value[key], old[key])


if __name__ == '__main__':
    unittest.main()
