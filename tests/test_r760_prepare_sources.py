from __future__ import annotations

import ast
import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PREPARE_PATH = ROOT / "deployment" / "r760-crawler" / "prepare_sources.py"


def load_prepare_module():
    spec = importlib.util.spec_from_file_location("r760_prepare_sources", PREPARE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_abn_sources_are_transformed_without_active_sensitive_literals(tmp_path) -> None:
    module = load_prepare_module()
    builder = module.BundleBuilder(
        ROOT / "dome1",
        tmp_path / "app",
        tmp_path / "secrets.json",
    )

    products = builder.transform_script("ABN2025_products_new.py")
    reports = builder.transform_script("ABN2025_new.py")

    ast.parse(products)
    ast.parse(reports)
    assert 'secret("SQL_ODBC_ABN_PRODUCTS")' in products
    assert 'secret("SQL_ODBC_ABN_REPORTS")' in reports
    assert 'secret("SMTP_PASSWORD_ABN")' in reports
    assert {
        "SQL_ODBC_ABN_PRODUCTS",
        "SQL_ODBC_ABN_REPORTS",
        "SMTP_PASSWORD_ABN",
    } <= set(builder.secrets)
    for value in builder.banned_values:
        assert not value or value not in products
        assert not value or value not in reports


def test_prepare_sources_preserves_existing_secrets_owner() -> None:
    source = PREPARE_PATH.read_text(encoding="utf-8")

    assert "existing_owner = (current.st_uid, current.st_gid)" in source
    assert 'os.chown(temporary, *existing_owner)' in source


def test_stbg_transform_disables_shared_ftp_keepalive_by_default(tmp_path) -> None:
    module = load_prepare_module()
    builder = module.BundleBuilder(
        ROOT / "dome1",
        tmp_path / "app",
        tmp_path / "secrets.json",
    )

    transformed = builder.transform_script("stbg_2025.py")

    ast.parse(transformed)
    assert 'os.environ.get("STBG_FTP_KEEPALIVE", "0")' in transformed
    assert "if ENABLE_STBG_FTP_KEEP_ALIVE:" in transformed
    assert "STBG FTP keep-alive threads disabled" in transformed
    assert "FTP directory list failed for {path}" in transformed
    assert "FTP upload failed for {ftp_file_path}" in transformed
    assert transformed.count("attach_ftp_config(") == 2
    assert "reconnect_ftp_connection(ftp, timeout=120)" in transformed
    assert "ftp.timeout = 120" in transformed
    assert "ftp.timeout = original_timeout" in transformed
    assert "ftp.sock.settimeout(120)" in transformed
    assert "ftp.sock.settimeout(original_socket_timeout)" in transformed
    assert "No trustee-report products to process" in transformed
    assert "skipping FTP product-directory scan" in transformed
    assert "timeout=600" not in transformed


def test_fxjg_transform_is_secret_free_and_safe_for_zero_increment(tmp_path) -> None:
    module = load_prepare_module()
    builder = module.BundleBuilder(
        ROOT / "dome1",
        tmp_path / "app",
        tmp_path / "secrets.json",
    )

    transformed = builder.transform_script("day_fxjg2023_new.py")

    ast.parse(transformed)
    assert 'secret("SQL_ODBC_FXJG")' in transformed
    assert 'os.environ.get("DEALVIEWER_PROXY_URL", "")' in transformed
    assert 'os.environ.get("FXJG_FTP_KEEPALIVE", "0")' in transformed
    assert "FXJG FTP keep-alive threads disabled" in transformed
    assert "FTP directory list failed for {path}" in transformed
    assert "FTP upload failed for {ftp_file_path}" in transformed
    assert transformed.count("attach_ftp_config(") == 2
    assert "reconnect_ftp_connection(ftp, timeout=120)" in transformed
    assert "No issuance-result products to process" in transformed
    assert "FXJG_LAST_DATE_OVERRIDE" in transformed
    assert "FXJG timestamp write disabled for canary" in transformed
    assert "increment_pdf_path = os.path.join(" in transformed
    assert 'product["title"] + ".success"' in transformed
    assert 'product["title"] + ".error"' in transformed
    assert "Issuance-result products were not fully processed" in transformed
    assert "timeout=600" not in transformed
    assert "SQL_ODBC_FXJG" in builder.secrets
    for value in builder.banned_values:
        assert not value or value not in transformed


def test_all_manual_crawler_tasks_are_exposed() -> None:
    source = (
        ROOT / "deployment" / "r760-crawler" / "app" / "run_crawler.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "ALLOWED_TASKS" for target in node.targets)
    )
    tasks = ast.literal_eval(assignment.value)

    assert {
        "abn-products",
        "abn-reports",
        "fxwj",
        "fxjg",
        "fxjg-canary",
        "stbg",
    } <= tasks


def test_runtime_wrapper_redacts_chinamoney_authentication_material() -> None:
    source = (
        ROOT / "deployment" / "r760-crawler" / "app" / "run_crawler.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_redact_child_line"
    )
    namespace: dict[str, object] = {}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "redactor", "exec"), namespace)
    redact = namespace["_redact_child_line"]

    assert "redacted" in redact("data = {'sign': 'temporary', 'UT': 'temporary'}\n")
    assert "redacted" in redact("Cookie: {'AlteonP10': 'temporary'}\n")
    assert "redacted" in redact("https://example/query?infoLevel=temporary&sign=temporary\n")
    assert redact("ordinary crawler output\n") == "ordinary crawler output\n"
