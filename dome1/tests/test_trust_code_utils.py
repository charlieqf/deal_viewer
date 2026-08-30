from trust_code_utils import build_trust_code, extract_issuance_series_name


def test_no_year_issuance_uses_short_series_prefix():
    product_name = "华驭第十八期汽车抵押贷款支持证券"

    series_name = extract_issuance_series_name(product_name)
    trust_code = build_trust_code("HuaYu-18", "AUTO")

    assert series_name == "华驭"
    assert trust_code == "HuaYu_AUTO-18"
    assert len(trust_code) <= 50


def test_year_issuance_keeps_year_in_series_prefix():
    product_name = "建欣2026年第十一期不良资产支持证券"

    series_name = extract_issuance_series_name(product_name)
    trust_code = build_trust_code("JianXin2026-11", "NPL")

    assert series_name == "建欣2026"
    assert trust_code == "JianXin_NPL2026-11"


def test_series_name_falls_back_to_full_name_without_markers():
    assert extract_issuance_series_name("  示例产品  ") == "示例产品"
