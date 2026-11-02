"""Read local issuer PDFs without changing their bytes; isolate each PDF parser."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
from text_quality import broken_character_map


METRIC_TERMS = {
    "total_assets": ["资产总额", "资产总计", "资产合计", "总资产"],
    "capital_adequacy_ratio": ["资本充足率"],
    "nonperforming_loan_ratio": ["不良贷款率", "不良贷款比率"],
    "provision_coverage_ratio": ["拨备覆盖率", "贷款减值准备对不良贷款比率"],
    "operating_revenue": ["营业收入", "营业总收入"],
    "total_profit": ["利润总额", "税前利润"],
    "net_profit": ["净利润", "税后利润"],
    "net_profit_attributable_to_parent": ["归属于母公司股东的净利润", "归属于本行股东的净利润", "归属于银行股东的净利润", "归属于本银行股东的净利润", "归属于母公司所有者的净利润", "归属于本行股东净利润", "归属于母公司净利润", "归属母公司股东的净利润"],
}
METRIC_TERMS['net_profit_attributable_to_parent'] += [
    '归属于上市公司股东的净利润', '归属于上市公司股东净利润', '归属于公司股东净利润',
    '归属于母公司股东净利润', '归属于本行股东的合并净利润',
    '归属于母公司股东的当期净利润', '归属于母公司股东当期净利润',
    '归属于母公司股东的当年净利润', '归属于母公司股东的本期净利润',
]


def inspect_pdf(path: Path, out: Path, deps: Path | None) -> dict:
    if deps:
        sys.path.insert(0, str(deps))
    import pymupdf

    hasher = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            hasher.update(block)
    digest = hasher.hexdigest()
    doc = pymupdf.open(path)
    if doc.needs_pass:
        raise ValueError("encrypted PDF requires a password")
    pages = []; began = time.monotonic()
    for index, page in enumerate(doc):
        content = page.get_text("text", sort=True)
        compact = "".join(content.split())
        found = [code for code, terms in METRIC_TERMS.items() if any(t in compact for t in terms)]
        image_rects = [pymupdf.Rect(x['bbox']) & page.rect for x in page.get_image_info()]
        image_area = min(1, sum(max(0,r.width)*max(0,r.height) for r in image_rects)
                         / max(1,page.rect.width*page.rect.height))
        replacement_ratio = content.count('\ufffd') / max(1, len(content))
        # Large image + sparse native text catches scanned tables with a native footer.
        needs_ocr = (image_area > .35 and len(compact) < 1000) or replacement_ratio > .02 or broken_character_map(content)
        pages.append({
            "page": index + 1, "text": content, "characters": len(compact),
            "metric_hits": found, "width": page.rect.width, "height": page.rect.height,
            "image_area_ratio": round(image_area, 4), "needs_ocr": needs_ocr,
            "coverage_status": 'ocr_required' if needs_ocr else 'native' if len(compact) >= 40 else 'sparse_no_large_image',
        })
    result = {
        "source_name": path.name, "sha256": digest, "bytes": path.stat().st_size,
        "page_count": len(doc), "pages": pages,
        "text_engine": "pymupdf-" + pymupdf.VersionBind,
        "layout_version": "page-coverage-v2", "elapsed_seconds": round(time.monotonic()-began,3),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / (digest + ".json")).write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    return {
        "source_name": path.name, "sha256": digest, "bytes": result["bytes"],
        "page_count": len(doc), "low_text_pages": sum(p["characters"] < 40 for p in pages),
        "ocr_pages": [p['page'] for p in pages if p['needs_ocr']],
        "native_seconds": result['elapsed_seconds'],
        "metric_pages": {code: [p["page"] for p in pages if code in p["metric_hits"]] for code in METRIC_TERMS},
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--deps", type=Path)
    parser.add_argument("--single", action="store_true")
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    if args.single:
        print(json.dumps(inspect_pdf(args.source, args.out, args.deps), ensure_ascii=False))
        return
    args.out.mkdir(parents=True, exist_ok=True)
    summaries = []
    for path in sorted(args.source.glob("*.pdf")):
        command = [sys.executable, str(Path(__file__).resolve()), "--single", "--source", str(path), "--out", str(args.out)]
        if args.deps:
            command += ["--deps", str(args.deps.resolve())]
        try:
            proc = subprocess.run(command, capture_output=True, encoding="utf-8", timeout=90)
            if proc.returncode:
                raise RuntimeError(proc.stderr[-1000:])
            result = json.loads(proc.stdout)
        except (subprocess.TimeoutExpired, RuntimeError) as exc:
            result = {"source_name": path.name, "error": str(exc)}
        summaries.append(result)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        (args.out / "manifest.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
