from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "dome1" / "trust_code_utils.py"


def load_module():
    assert MODULE_PATH.exists(), f"Missing helper module: {MODULE_PATH}"
    spec = importlib.util.spec_from_file_location("trust_code_utils", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_trust_code_inserts_asset_type_before_dynamic_year() -> None:
    module = load_module()

    assert (
        module.build_trust_code("PuXinGuiHang2026-3", "NPL") == "PuXinGuiHang_NPL2026-3"
    )


def test_build_trust_code_preserves_existing_2025_pattern() -> None:
    module = load_module()

    assert module.build_trust_code("HuaYu2025-17", "AUTO") == "HuaYu_AUTO2025-17"
