"""Shared presentation contract: an empty result is not a review task."""
import unittest
from preinvestment import KINDS, SOURCES, item_statuses


class ItemStatusTests(unittest.TestCase):
    def setUp(self):
        self.summary = dict(WindowStart='2024-11-06', BusinessDate='2026-11-06',
                            SuspensionDeduction=0, ServicerDeduction=0, MaturityDeduction=0)
        self.sources = [dict(code=code, label=code, latest=dict(
            Status='success', CheckedAt='2026-11-06 03:00:00',
            QueryStart='2024-11-06', QueryEnd='2026-11-06', PendingDocuments=0))
            for codes in SOURCES.values() for code in codes]

    def result(self, events=(), sources=None):
        return item_statuses(events, self.sources if sources is None else sources, self.summary)

    def test_successful_empty_search_needs_no_review_for_every_category(self):
        for item in self.result().values():
            self.assertEqual(item['status'], 'not_detected')
            self.assertFalse(item['requires_review'])
            self.assertEqual(item['suggested_deduction'], 0)
            self.assertEqual(item['query_end'], '2026-11-06')

    def test_missing_source_is_not_a_negative_finding(self):
        for code in [source['code'] for source in self.sources]:
            with self.subTest(source=code):
                result = self.result(sources=[s for s in self.sources if s['code'] != code])
                kind = next(k for k in KINDS if code in SOURCES[k])
                self.assertEqual(result[kind]['status'], 'not_queried')
                self.assertFalse(result[kind]['requires_review'])
        self.assertTrue(all(x['status'] == 'not_queried' for x in self.result(sources=[]).values()))

    def test_latest_failure_does_not_fall_back_to_old_success(self):
        source = self.sources[0]
        source['success'] = dict(source['latest'])
        source['latest']['Status'] = 'failed'
        item = self.result()['abs_suspension']
        self.assertEqual(item['status'], 'query_failed')
        self.assertFalse(item['requires_review'])

    def test_not_connected_is_distinct_from_query_failure(self):
        self.sources[0]['latest']['Status'] = 'not_connected'
        self.assertEqual(self.result()['abs_suspension']['status'], 'not_connected')

    def test_backlog_is_category_specific_and_not_a_review_task(self):
        self.sources[0]['latest']['PendingDocuments'] = 2
        self.sources[1]['latest']['PendingDocuments'] = 2
        result = self.result()
        self.assertEqual(result['abs_suspension']['status'], 'pending_processing')
        self.assertEqual(result['abs_suspension']['pending_documents'], 2)
        self.assertFalse(result['abs_suspension']['requires_review'])
        self.assertEqual(result['servicer_failure']['status'], 'not_detected')
        # Unrelated punishment/rating coverage must not block these categories.
        self.sources.append(dict(code='qcc', latest=dict(Status='not_connected', PendingDocuments=500)))
        self.assertEqual(self.result()['servicer_failure']['status'], 'not_detected')

    def test_only_actual_pending_events_require_review(self):
        for kind in KINDS:
            with self.subTest(kind=kind):
                item = self.result([dict(Category=kind, Status='pending', EventDate=None)])[kind]
                self.assertEqual(item['status'], 'pending_review')
                self.assertTrue(item['requires_review'])
                self.assertEqual(item['suggested_deduction'], 0)

    def test_confirmed_risk_remains_visible_alongside_pending_work(self):
        self.summary['ServicerDeduction'] = 5
        self.sources[2]['latest']['Status'] = 'failed'
        events = [dict(Category='servicer_failure', Status='confirmed', EventDate='2024-11-06'),
                  dict(Category='servicer_failure', Status='pending', EventDate=None)]
        item = self.result(events)['servicer_failure']
        self.assertEqual(item['status'], 'confirmed')
        self.assertEqual(item['confirmed_events'], 1)
        self.assertEqual(item['pending_events'], 1)
        self.assertTrue(item['requires_review'])
        self.assertEqual(item['suggested_deduction'], 5)
        self.assertEqual(item['sources'][0]['status'], 'failed')

    def test_excluded_and_out_of_window_events_do_not_create_review_tasks(self):
        events = [dict(Category='servicer_failure', Status=status, EventDate=day)
                  for status, day in [('excluded', '2026-11-01'), ('confirmed', '2024-11-05'),
                                      ('confirmed', '2026-11-07')]]
        item = self.result(events)['servicer_failure']
        self.assertEqual(item['status'], 'not_detected')
        self.assertFalse(item['requires_review'])

    def test_as_of_uses_oldest_required_source(self):
        self.sources[1]['latest']['QueryEnd'] = '2026-11-03'
        self.assertEqual(self.result()['abs_suspension']['query_end'], '2026-11-03')


if __name__ == '__main__':
    unittest.main()
