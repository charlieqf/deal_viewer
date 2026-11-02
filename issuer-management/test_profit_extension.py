import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from profit_extension import BASE_CODES, PROFIT_CODES, load_baseline


class BaselineTests(unittest.TestCase):
    def test_baseline_identity_digest_and_ambiguity(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'input').mkdir()
            data={'issuer_name':'测试银行','records':[{'FinancialRecordId':'5','PeriodEnd':'2025-12-31'}],
                  'metrics':[{'FinancialRecordId':'5','MetricCode':code,'ObservationId':str(i+1),'ValueStatus':'reported','ReviewStatus':'pending'} for i,code in enumerate(sorted(BASE_CODES))]}
            def save():
                raw=json.dumps(data).encode();(root/'input/baseline.json').write_bytes(raw)
                return {'extract_only':PROFIT_CODES,'baseline_sha256':hashlib.sha256(raw).hexdigest()}
            config=save();baseline,ids=load_baseline(root,config,'测试银行')
            self.assertEqual(len(baseline),6);self.assertEqual(ids,list(range(1,7)))
            with self.assertRaisesRegex(ValueError,'issuer'):load_baseline(root,config,'其他银行')
            with self.assertRaisesRegex(ValueError,'digest'):load_baseline(root,dict(config,baseline_sha256='bad'),'测试银行')
            data['records'].append({'FinancialRecordId':'6','PeriodEnd':'2025-12-31'})
            with self.assertRaisesRegex(ValueError,'one record'):load_baseline(root,save(),'测试银行')
            data['records'].pop();data['metrics'][0]['ReviewStatus']='verified'
            with self.assertRaisesRegex(ValueError,'Reviewed'):load_baseline(root,save(),'测试银行')

    def test_primary_run_has_no_baseline_dependency(self):
        self.assertEqual(load_baseline(Path('/absent'),{},'测试银行'),({},[]))
