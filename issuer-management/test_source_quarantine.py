import json,tempfile,unittest
from pathlib import Path
from source_quarantine import quarantined

class QuarantineTests(unittest.TestCase):
    def test_requires_exact_source_and_truncation_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'input').mkdir()
            doc=dict(page_count=0,sha256='a',source_name='report.pdf',bytes=10)
            with self.assertRaises(ValueError):quarantined(doc,root)
            e=dict(sha256='a',source_name='report.pdf',actual_bytes=10,declared_bytes=20,
                   reason_code='truncated_pdf',source_retained=True,independent_parser_failures=['pypdf','pdfium'])
            path=root/'input/source-quarantine.json';path.write_text(json.dumps({'files':[e]}))
            self.assertTrue(quarantined(doc,root))
            self.assertFalse(quarantined(dict(doc,page_count=1),root))
            e['declared_bytes']=10;path.write_text(json.dumps({'files':[e]}))
            with self.assertRaises(ValueError):quarantined(doc,root)

if __name__=='__main__':unittest.main()
