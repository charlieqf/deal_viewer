from __future__ import annotations

import re


YEAR_PATTERN = re.compile(r"20\d{2}")


def build_trust_code(base_trust_code: str, asset_code: str) -> str:
    match = YEAR_PATTERN.search(base_trust_code)
    if match:
        insert_at = match.start()
    else:
        insert_at = base_trust_code.rfind("-")
        if insert_at == -1:
            return (
                base_trust_code if not asset_code else f"{base_trust_code}_{asset_code}"
            )

    prefix = base_trust_code[:insert_at]
    suffix = base_trust_code[insert_at:]
    asset_segment = f"_{asset_code}" if asset_code else "_"
    return f"{prefix}{asset_segment}{suffix}"
