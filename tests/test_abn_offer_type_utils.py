from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "dome1" / "abn_offer_type_utils.py"


def load_module():
    assert MODULE_PATH.exists(), f"Missing helper module: {MODULE_PATH}"
    spec = importlib.util.spec_from_file_location("abn_offer_type_utils", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_private_equity_offer_type_variants_map_to_private_bucket() -> None:
    module = load_module()

    result = module.classify_offer_type("定向投资人")

    assert result.collection_method == "PrivateEquity"
    assert result.summary_bucket == "private"


def test_public_offering_offer_type_maps_to_public_bucket() -> None:
    module = load_module()

    result = module.classify_offer_type("公开发行")

    assert result.collection_method == "PublicOffering"
    assert result.summary_bucket == "public"


def test_unknown_offer_type_still_defaults_to_public_bucket() -> None:
    module = load_module()

    result = module.classify_offer_type("合格机构投资者")

    assert result.collection_method == "PublicOffering"
    assert result.summary_bucket == "public"
