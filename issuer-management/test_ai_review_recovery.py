import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from ai_review_batch import PartialReviewFailure, Runner, db


def record(record_id, metrics):
    return {'FinancialRecordId': record_id, 'LegalName': 'Test bank',
            'InstitutionType': 'bank', 'evidence_pages': {},
            'metrics': [{'MetricCode': code, 'ValueStatus': 'reported', 'ObservationId': i}
                        for i, code in enumerate(metrics)]}


def context(root, records, expanded=False):
    return {'records': [{'record_id': r['FinancialRecordId'],
                         'requested': [m['MetricCode'] for m in r['metrics']]}
                        for r in records], 'pages': []}


def failure(code, allowed=True):
    return {'status': 502, 'body': {'error': {'code': code,
                                            'transformed_retry_allowed': allowed}}}


def success(payload):
    ctx = json.loads(payload['messages'][-1]['content'])
    result = {'refs': {}, 'items': [{'record_id': r['record_id'], 'metric_code': code,
                                    'evidence': {}}
                                   for r in ctx['records'] for code in r['requested']]}
    return {'status': 200, 'body': {'choices': [{'finish_reason': 'tool_calls', 'message': {
        'tool_calls': [{'function': {'name': 'submit_financial_review',
                                     'arguments': json.dumps(result)}}]}}]}}


class ReviewRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.runner = Runner.__new__(Runner)
        self.runner.root = Path(self.temp.name)
        self.runner.lock = threading.Lock()
        db(self.runner.root).close()
        self.context_patch = patch('ai_review_batch.context', side_effect=context)
        self.context_patch.start()
        self.addCleanup(self.context_patch.stop)

    def test_truncation_keeps_successful_siblings_and_continues_after_failed_leaf(self):
        records = [record(1, ['a', 'b', 'c', 'd'])]
        seen = []
        def respond(key, payload):
            metrics = json.loads(payload['messages'][-1]['content'])['records'][0]['requested']
            seen.append(metrics)
            return failure('tool_call_output_truncated') if 'a' in metrics else success(payload)
        with patch.object(self.runner, 'response', side_effect=respond):
            with self.assertRaises(PartialReviewFailure) as caught:
                self.runner.call('goldencode-d', None, records, 'initial')
        self.assertEqual(set(caught.exception.answers), {(1, 'b'), (1, 'c'), (1, 'd')})
        self.assertEqual(caught.exception.failures, {(1, 'a'): 'model_single_metric_output_limit'})
        self.assertEqual(seen, [['a', 'b', 'c', 'd'], ['a', 'b'], ['a'], ['a'], ['b'], ['c', 'd']])

    def test_single_metric_failure_does_not_skip_later_report(self):
        records = [record(1, ['a']), record(2, ['b'])]
        def respond(key, payload):
            first = json.loads(payload['messages'][-1]['content'])['records'][0]['record_id']
            return failure('tool_call_output_truncated') if first == 1 else success(payload)
        with patch.object(self.runner, 'response', side_effect=respond) as response:
            with self.assertRaises(PartialReviewFailure) as caught:
                self.runner.call('goldencode-d', None, records, 'expanded')
        self.assertEqual(response.call_count, 3)
        self.assertEqual(set(caught.exception.answers), {(2, 'b')})
        self.assertEqual(set(caught.exception.failures), {(1, 'a')})

    def test_single_metric_truncation_recovers_once_with_bounded_plain_json(self):
        payloads = []
        def respond(key, payload):
            payloads.append(json.loads(json.dumps(payload)))
            if 'tools' in payload: return failure('tool_call_output_truncated')
            body = success(payload)['body']
            message = body['choices'][0]['message']
            message['content'] = message.pop('tool_calls')[0]['function']['arguments']
            body['choices'][0]['finish_reason'] = 'stop'
            return {'status': 200, 'body': body}
        with patch.object(self.runner, 'response', side_effect=respond):
            answers = self.runner.call('goldencode-d', None, [record(1, ['a'])], 'initial')
        self.assertEqual(set(answers), {(1, 'a')})
        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[1]['max_tokens'], 16000)
        self.assertNotIn('tools', payloads[1]); self.assertNotIn('tool_choice', payloads[1])
        self.assertEqual(payloads[0]['messages'][-1], payloads[1]['messages'][-1])
        self.assertEqual(len(list((self.runner.root / 'requests').glob('*.json'))), 2)

    def test_single_metric_text_limit_is_terminal_after_two_generations(self):
        with patch.object(self.runner, 'response', return_value=failure('tool_call_output_truncated')) as response:
            with self.assertRaisesRegex(ValueError, 'model_single_metric_output_limit'):
                self.runner.call('goldencode-d', None, [record(1, ['a'])], 'initial')
        self.assertEqual(response.call_count, 2)

    def test_invalid_text_recovery_is_rejected_without_another_retry(self):
        bad = {'status': 200, 'body': {'choices': [{'finish_reason': 'stop',
               'message': {'content': '{"refs": {}, "items": []}'}}]}}
        with patch.object(self.runner, 'response', side_effect=[failure('tool_call_output_truncated'), bad]) as response:
            with self.assertRaisesRegex(ValueError, 'invalid_single_metric_recovery'):
                self.runner.call('goldencode-d', None, [record(1, ['a'])], 'initial')
        self.assertEqual(response.call_count, 2)

    def test_json_failure_splits_after_one_corrected_retry(self):
        records = [record(1, ['a', 'b'])]
        payloads = []
        def respond(key, payload):
            payloads.append(json.loads(json.dumps(payload)))
            count = len(json.loads(payload['messages'][-1]['content'])['records'][0]['requested'])
            return failure('tool_call_validation_failed') if count > 1 else success(payload)
        with patch.object(self.runner, 'response', side_effect=respond):
            answers = self.runner.call('goldencode', None, records, 'initial')
        self.assertEqual(set(answers), {(1, 'a'), (1, 'b')})
        self.assertEqual(len(payloads), 4)
        self.assertNotEqual(payloads[0]['messages'][0]['content'], payloads[1]['messages'][0]['content'])
        self.assertEqual({p['max_tokens'] for p in payloads}, {64000})
        self.assertEqual([p['tools'][0]['function']['parameters']['properties']['items']['maxItems']
                          for p in payloads], [2, 2, 1, 1])

    def test_single_metric_json_failure_is_bounded(self):
        with patch.object(self.runner, 'response', return_value=failure('tool_call_validation_failed')) as response:
            with self.assertRaisesRegex(ValueError, 'model_tool_call_validation_failed'):
                self.runner.call('goldencode', None, [record(1, ['a'])], 'initial')
        self.assertEqual(response.call_count, 2)

    def test_truncation_after_json_retry_still_splits(self):
        records = [record(1, ['a', 'b'])]
        count = 0
        def respond(key, payload):
            nonlocal count
            count += 1
            if count == 1: return failure('tool_call_validation_failed')
            if count == 2: return failure('tool_call_output_truncated')
            return success(payload)
        with patch.object(self.runner, 'response', side_effect=respond):
            self.assertEqual(len(self.runner.call('goldencode', None, records, 'initial')), 2)
        self.assertEqual(count, 4)

    def test_no_transformed_retry_without_gateway_permission(self):
        with patch.object(self.runner, 'response', return_value=failure('tool_call_output_truncated', False)) as response:
            with self.assertRaisesRegex(ValueError, 'model_tool_call_output_truncated'):
                self.runner.call('goldencode-d', None, [record(1, ['a', 'b'])], 'initial')
        self.assertEqual(response.call_count, 1)

    def test_job_marks_only_failed_metric_and_keeps_other_model_evidence(self):
        self.runner.records = {1: record(1, ['a', 'b'])}
        answers = {(1, code): {'metric_code': code} for code in ['a', 'b']}
        def call(model, *args):
            if model == 'goldencode': return answers
            raise PartialReviewFailure({(1, 'b'): answers[(1, 'b')]},
                                       {(1, 'a'): 'model_single_metric_output_limit'})
        with patch.object(self.runner, 'call', side_effect=call), patch('ai_review_batch.assess', return_value=[]), patch.object(self.runner, 'progress'):
            self.runner.job({'id': '1', 'record_ids': [1]})
        result = json.loads((self.runner.root / 'results/1.json').read_text())
        by_metric = {item['metric']: item for item in result['results']}
        self.assertEqual(by_metric['a']['status'], 'request_failed')
        self.assertEqual(by_metric['b']['status'], 'checks_passed')
        self.assertEqual(by_metric['a']['findings']['goldencode'], answers[(1, 'a')])
        self.assertEqual(by_metric['b']['findings']['goldencode-d'], answers[(1, 'b')])


if __name__ == '__main__':
    unittest.main()
