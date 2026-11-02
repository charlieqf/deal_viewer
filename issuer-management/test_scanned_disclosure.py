import hashlib,unittest
from production_review_scanned_disclosures import overlay

class TranscriptionTests(unittest.TestCase):
    def item(self,raw='123.45'):
        text='集团利润表 2024年1-3月 金额单位：万元\n营业收入 123.45'
        return dict(sha256='a',page=2,raw_value=raw,visual_checked=True,
                    image_transcript=dict(page=2,text=text,sha256=hashlib.sha256(text.encode()).hexdigest()))
    def test_keeps_ocr_and_distinguishes_transcription(self):
        pages={2:dict(page=2,method='paddleocr',text='营业收 123.45',ocr_artifact='old.json')}
        result=overlay(pages,{'sha256':'a'},[self.item()])
        self.assertEqual(pages[2]['text'],'营业收 123.45')
        self.assertEqual(result[2]['original_extracted_text'],pages[2]['text'])
        self.assertEqual(result[2]['method'],'visual_transcription')
        self.assertNotIn('ocr_artifact',result[2])
    def test_cannot_invent_value_or_change_transcript_after_hashing(self):
        pages={2:dict(page=2,text='营业收 123.45')}
        with self.assertRaises(AssertionError):overlay(pages,{'sha256':'a'},[self.item('987.65')])
        item=self.item();item['image_transcript']['text']+='0'
        with self.assertRaises(AssertionError):overlay(pages,{'sha256':'a'},[item])

if __name__=='__main__':unittest.main()
