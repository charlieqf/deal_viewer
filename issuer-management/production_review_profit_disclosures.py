"""Import visually checked profit evidence without requesting the model again."""
import argparse,gzip,hashlib,json,re
from pathlib import Path
import production_review_disclosures as review
from pilot_runtime import ROOT,ARTIFACTS,atomic_json,encode

ENGINE='profit_disclosure_review'


def overlay(pages,doc,items):
    """Append pinned image transcriptions; retain original OCR, never invent digits."""
    result={n:dict(p) for n,p in pages.items()}
    rows={}
    for item in items:
        transcript=item.get('image_transcript')
        if item['sha256']!=doc['sha256'] or not transcript:continue
        c=item['candidate'];pn=item['page'];raw=c['raw_value']
        assert item['visual_checked'] is True and transcript['page']==pn
        assert transcript['sha256']==hashlib.sha256(transcript['text'].encode()).hexdigest()
        # OCR sometimes inserts whitespace after a thousands comma. Only join that
        # separator, never concatenate independent numeric columns or repair digits.
        original=re.sub(r'(?<=,)\s+(?=\d{3}(?:\D|$))','',pages[pn]['text'])
        assert re.search(r'(?<![\d.,])'+re.escape(raw)+r'(?![\d.,])',original), 'Numeric token absent from original OCR'
        assert raw in transcript['text'] and c['original_label'] in transcript['text']
        rows.setdefault(pn,[]).append(transcript['text'])
    for pn,texts in rows.items():
        result[pn].update(method='visual_transcription',original_extracted_text=pages[pn]['text'],
            text=pages[pn]['text']+'\n\n[原图逐行核对转录；仍待人工业务复核]\n'+'\n'.join(dict.fromkeys(texts)))
        result[pn].pop('ocr_artifact',None)
    return result


def require_manifest(manifest,wrapper_hash):
    assert manifest['items'], 'No reviewed profit evidence supplied'
    for item in manifest['items']:
        c=item['candidate']
        assert item.get('review_wrapper_sha256')==wrapper_hash
        assert item.get('visual_checked') is True and len(item.get('image_sha256',''))==64
        assert c['metric_code'] in ('net_profit','net_profit_attributable_to_parent')
        assert item['metric_code']==c['metric_code']
        assert c['entity_scope']=='accounting_consolidated' and c['currency']=='CNY'
        assert c['method']=='not_applicable' and c['period_basis']=='year_to_date'
        assert c['period_start']==c['period_end'][:4]+'-01-01'


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--manifest',required=True);args=parser.parse_args()
    manifest=json.loads(Path(args.manifest).read_text(encoding='utf-8'))
    require_manifest(manifest,hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    for item in manifest['items']:
        image=ROOT/'input/profit-review-images'/f"{item['document_id']}-{item['page']}.png"
        assert hashlib.sha256(image.read_bytes()).hexdigest()==item['image_sha256']
    original=review.merged_pages
    review.merged_pages=lambda doc:overlay(original(doc),doc,manifest['items'])
    review.ENGINE=ENGINE
    review.main()
    bundle=json.loads((ROOT/'normalized-bundle.json').read_text())
    for attempt in bundle['attempts']:
        if attempt.get('engine')!=ENGINE:continue
        for c in attempt['candidates']:
            assert c['metric_code'] in ('net_profit','net_profit_attributable_to_parent') and c['method']=='not_applicable'
            c['measurement_label']='not_applicable';c['measurement_code']='not_applicable'
        attempt['metadata']['scope_review']='accounting_consolidated'
    atomic_json(ROOT/'normalized-bundle.json',bundle)
    compressed=gzip.compress(encode(bundle))
    (ROOT/'normalized-bundle.json.gz').write_bytes(compressed)
    (ROOT/'input/normalized-bundle.json.gz').write_bytes(compressed)


if __name__=='__main__':main()
