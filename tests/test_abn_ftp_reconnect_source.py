from pathlib import Path


SOURCE = Path(__file__).resolve().parents[1] / "dome1" / "ABN2025_new.py"


def test_abn_report_reconnect_uses_attached_session_credentials() -> None:
    source = SOURCE.read_text(encoding="utf-8-sig")

    assert "attach_ftp_config" in source
    assert source.count("reconnect_ftp_connection(ftp,") == 2
    assert "ftp._user" not in source
    assert "ftp._passwd" not in source
    assert "host=FTP2_HOST" in source
    assert "user=FTP2_USER" in source
    assert "password=FTP2_PASS" in source


def test_abn_report_product_prefix_fallback_requires_a_unique_match() -> None:
    source = SOURCE.read_text(encoding="utf-8-sig")

    assert "select top 2 trustid,trustcode" in source
    assert "cur.execute(sql_prefix, product_prefix + \"%\")" in source
    assert "if len(prefix_matches) == 1:" in source
    assert "elif len(prefix_matches) > 1:" in source


def test_abn_report_prefers_complete_name_before_report_marker() -> None:
    source = SOURCE.read_text(encoding="utf-8-sig")

    assert '("信托资产运营报告", "信托财产运营报告")' in source
    assert "prod = title[:report_pos].rstrip()" in source
    assert "sub_title = title[report_pos:]" in source
