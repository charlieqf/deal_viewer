"""Import explicitly image-checked row transcriptions, retaining unmodified OCR evidence."""
import hashlib,json,sys
from pathlib import Path
import production_review_disclosures as review
from pilot_runtime import ROOT

def overlay(pages,doc,items):
    result={n:dict(p) for n,p in pages.items()}
    by_page={}
    for item in items:
        if item['sha256']!=doc['sha256']:continue
        transcript=item.get('image_transcript')
        if not transcript:continue
        assert item['visual_checked'] is True
        assert item['raw_value'] in pages[item['page']]['text'], 'Transcription cannot invent or repair the numeric token'
        assert transcript['page']==item['page'] and len(transcript['text'])>20
        assert transcript['sha256']==hashlib.sha256(transcript['text'].encode()).hexdigest()
        by_page.setdefault(item['page'],[]).append(transcript['text'])
    for pn,rows in by_page.items():
        original=pages[pn]
        result[pn]=dict(original,method='visual_transcription',original_extracted_text=original['text'],
            text=original['text']+'\n\n[原图逐行核对转录；AI核对，仍待人工业务复核]\n'+'\n'.join(dict.fromkeys(rows)))
        result[pn].pop('ocr_artifact',None)  # A transcription has no automatic OCR bounding boxes.
    return result

def main():
    manifest=Path(sys.argv[sys.argv.index('--manifest')+1]);items=json.loads(manifest.read_text())['items']
    for item in items:
        if not item.get('image_transcript'):continue
        image=ROOT/'input/disclosure-images'/f"{item['document_id']}-{item['page']}.png"
        assert hashlib.sha256(image.read_bytes()).hexdigest()==item['visual_sha256']
        assert item['transcription_code_sha256']==hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    original=review.merged_pages
    review.merged_pages=lambda doc:overlay(original(doc),doc,items)
    review.main()

if __name__=='__main__':main()
