"""OCR a bounded PDF page manifest; cache includes file, page, engine and render settings."""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(os.environ.get('ISSUER_PILOT_ROOT', '/pilot'))
DPI=int(os.environ.get('OCR_DPI','200')); LIMIT=int(os.environ.get('OCR_DET_LIMIT','1536'))
PROFILE = f'ppocr5-mobile-paddle3.2.2-gray{DPI}dpi-det{LIMIT}-v2'
ROTATION=int(os.environ.get('OCR_ROTATION','0'))
assert ROTATION in (0,90,180,270)
if ROTATION:PROFILE+=f'-rotate{ROTATION}'


def main():
    # Cache-only replays do not import Paddle or initialize its models.
    model=None; init_seconds=0
    manifest = json.loads((ROOT/'input'/os.environ.get('OCR_MANIFEST','ocr-manifest.json')).read_text())
    shard=int(os.environ.get('OCR_SHARD_INDEX','0')); shards=int(os.environ.get('OCR_SHARD_COUNT','1'))
    assert 0 <= shard < shards <= 4
    began = time.monotonic(); hits = 0; count = 0; seconds = []; index=0
    for source in manifest:
        path = ROOT/'input'/source['pdf']
        with path.open('rb') as f: assert hashlib.file_digest(f, 'sha256').hexdigest() == source['transfer_sha256']
        doc = None
        for item in source['pages']:
            index+=1
            if (index-1)%shards!=shard: continue
            key = hashlib.sha256(f"{item['sha256']}:{item['page']}:{PROFILE}".encode()).hexdigest()
            target = ROOT/'artifacts'/os.environ.get('OCR_OUTPUT','ocr')/item['sha256']/(str(item['page'])+'.json')
            if target.exists() and json.loads(target.read_text()).get('cache_key') == key:
                hits += 1; continue
            if model is None:
                init_start=time.monotonic()
                sys.path.insert(0, '/opt/meeting-ocr')
                from paddle_ocr import blocks_from, reading_order
                import pypdfium2 as pdfium
                with contextlib.redirect_stdout(sys.stderr):
                    from paddleocr import PaddleOCR
                    model = PaddleOCR(text_detection_model_name='PP-OCRv5_mobile_det', text_detection_model_dir='/opt/meeting-ocr/models/PP-OCRv5_mobile_det_infer', text_recognition_model_name='PP-OCRv5_mobile_rec', text_recognition_model_dir='/opt/meeting-ocr/models/PP-OCRv5_mobile_rec_infer', use_doc_orientation_classify=False, use_doc_unwarping=False, use_textline_orientation=False, device='cpu', cpu_threads=2, enable_mkldnn=True, text_det_limit_side_len=LIMIT, text_det_limit_type='max', text_rec_score_thresh=0.5)
                init_seconds=round(time.monotonic()-init_start,3)
            if doc is None:doc=pdfium.PdfDocument(str(path))
            start = time.monotonic(); page = doc[item['input_page']-1]
            width, height = page.get_size()
            bitmap = page.render(scale=DPI/72, grayscale=True)
            image = bitmap.to_pil().convert('RGB')
            if ROTATION:image=image.rotate(ROTATION,expand=True)
            image.save('/tmp/issuer-ocr-v2.png')
            with contextlib.redirect_stdout(sys.stderr): result = list(model.predict('/tmp/issuer-ocr-v2.png'))
            blocks = blocks_from(result[0], *image.size)
            elapsed = round(time.monotonic()-start, 3); seconds.append(elapsed)
            data = dict(item, text=reading_order(blocks), method='paddleocr', engine=PROFILE, cache_key=key,
                        width=image.width, height=image.height, original_width=width, original_height=height,
                        blocks=blocks, elapsed_seconds=elapsed,rotation_ccw=ROTATION)
            target.parent.mkdir(parents=True, exist_ok=True)
            part = target.with_suffix('.part'); part.write_text(json.dumps(data, ensure_ascii=False)); part.replace(target)
            page.close(); bitmap.close(); image.close(); count += 1
            print(json.dumps({'event':'ocr_page','page':item['page'],'sha256':item['sha256'],'seconds':elapsed,'characters':len(data['text'])}), flush=True)
        if doc is not None:doc.close()
    summary = {'profile':PROFILE,'processed':count,'cache_hits':hits,'page_seconds':round(sum(seconds),3),'wall_seconds':round(time.monotonic()-began,3),'shard':shard,'shards':shards,'model_init_seconds':init_seconds}
    name=os.environ.get('OCR_TIMING_NAME',f'{os.environ.get("OCR_OUTPUT","ocr")}-timing-{shard}-{shards}.json')
    (ROOT/'artifacts'/name).write_text(json.dumps(summary))
    print(json.dumps(summary), flush=True)


if __name__ == '__main__': main()
