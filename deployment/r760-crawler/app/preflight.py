from __future__ import annotations

import ftplib
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import pymssql
import pyodbc
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from dealviewer_runtime import (
    configured_secret_names,
    pymssql_connection_kwargs,
    secret,
    secret_int,
)


CHINABOND_URL = "https://www.chinabond.com.cn/cbiw/trs/getContentByConditions"
CHINAMONEY_URL = "https://www.chinamoney.com.cn/chinese/qwjsn/"


@dataclass
class CheckResult:
    name: str
    ok: bool
    elapsed_ms: int
    detail: dict[str, Any]


def _session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json",
            "Referer": "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/",
            "User-Agent": "DealViewer-R760-Preflight/1.0",
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    return session


def _chinabond_payload(report_name: str, page_size: int) -> dict[str, Any]:
    key = "excludeParentChnlNames" if report_name == "发行文件" else "excludeChnlNames"
    return {
        "parentChnlName": "zqzl_zjzzczj",
        key: [],
        "childChnlDesc": report_name,
        "hasAppendix": True,
        "siteName": "chinaBond",
        "pageSize": page_size,
        "pageNum": 1,
        "queryParam": {
            "keywords": "",
            "startDate": "",
            "endDate": "",
            "reportType": "",
            "reportYear": "",
            "ratingAgency": "",
        },
    }


def check_chinabond(report_name: str) -> dict[str, Any]:
    page_size = 50 if report_name == "发行文件" else 100
    with _session() as session:
        response = session.post(
            CHINABOND_URL,
            json=_chinabond_payload(report_name, page_size),
            timeout=(10, 30),
        )
        response.raise_for_status()
        body = response.json()
        if body.get("success") is not True:
            raise RuntimeError("Chinabond business response was not successful")
        items = body.get("data", {}).get("list", [])
        if not isinstance(items, list) or not items:
            raise RuntimeError("Chinabond returned no list items")

        pdf_checked = False
        for item in items:
            appendix_ids = item.get("appendixIds") or ""
            doc_url = item.get("docPubUrl") or ""
            parts = appendix_ids.split("=")
            if len(parts) < 2 or not doc_url:
                continue
            pdf_url = f"{doc_url.rsplit('/', 1)[0]}/{parts[1]}"
            with session.get(pdf_url, timeout=(10, 30), stream=True) as pdf_response:
                pdf_response.raise_for_status()
                first_bytes = next(pdf_response.iter_content(chunk_size=8), b"")
                content_type = pdf_response.headers.get("Content-Type", "").lower()
                if not (first_bytes.startswith(b"%PDF") or "pdf" in content_type):
                    raise RuntimeError("Chinabond sample attachment was not a PDF")
            pdf_checked = True
            break
        return {"items": len(items), "sample_pdf": pdf_checked, "mode": "direct"}


def check_chinamoney() -> dict[str, Any]:
    with _session() as session:
        response = session.get(CHINAMONEY_URL, timeout=(10, 30))
        response.raise_for_status()
        if len(response.content) < 1000:
            raise RuntimeError("ChinaMoney returned an unexpectedly small response")
        return {
            "status": response.status_code,
            "response_nonempty": True,
            "mode": "direct",
        }


def _ftp_check_once(prefix: str) -> dict[str, Any]:
    ftp = ftplib.FTP()
    try:
        ftp.connect(secret(f"{prefix}_HOST"), secret_int(f"{prefix}_PORT"), timeout=20)
        ftp.login(secret(f"{prefix}_USER"), secret(f"{prefix}_PASSWORD"))
        ftp.set_pasv(True)
        current_dir = ftp.pwd()
        names = None
        selected_encoding = None
        for encoding in ("utf-8", "gb18030"):
            ftp.encoding = encoding
            if encoding == "utf-8":
                try:
                    ftp.sendcmd("OPTS UTF8 ON")
                except ftplib.error_perm:
                    pass
            try:
                names = ftp.nlst()
                selected_encoding = encoding
                break
            except UnicodeDecodeError:
                continue
        if names is None:
            raise UnicodeError("FTP directory listing could not be decoded")
        ftp.voidcmd("NOOP")
        return {
            "cwd_nonempty": bool(current_dir),
            "entries": len(names),
            "encoding": selected_encoding,
            "writes": 0,
        }
    finally:
        try:
            ftp.quit()
        except Exception:
            ftp.close()


def _ftp_check(prefix: str) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            result = _ftp_check_once(prefix)
            result["attempts"] = attempt
            return result
        except (TimeoutError, OSError, EOFError, ftplib.Error, UnicodeError) as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(attempt)
    assert last_error is not None
    raise last_error


def check_ftp_primary() -> dict[str, Any]:
    return _ftp_check("FTP_PRIMARY")


def check_ftp_secondary() -> dict[str, Any]:
    return _ftp_check("FTP_SECONDARY")


def _odbc_check(secret_name: str) -> dict[str, Any]:
    connection = pyodbc.connect(secret(secret_name), timeout=12, autocommit=False)
    try:
        value = connection.cursor().execute("SELECT 1").fetchone()[0]
        connection.rollback()
        if int(value) != 1:
            raise RuntimeError("Unexpected SQL Server SELECT result")
        return {"select_one": True, "writes": 0}
    finally:
        connection.close()


def check_odbc_fxwj() -> dict[str, Any]:
    return _odbc_check("SQL_ODBC_FXWJ")


def check_odbc_fxjg() -> dict[str, Any]:
    return _odbc_check("SQL_ODBC_FXJG")


def check_odbc_stbg() -> dict[str, Any]:
    return _odbc_check("SQL_ODBC_STBG")


def check_odbc_abn_products() -> dict[str, Any]:
    return _odbc_check("SQL_ODBC_ABN_PRODUCTS")


def check_odbc_abn_reports() -> dict[str, Any]:
    return _odbc_check("SQL_ODBC_ABN_REPORTS")


def check_pymssql() -> dict[str, Any]:
    kwargs = pymssql_connection_kwargs()
    connection = pymssql.connect(**kwargs, login_timeout=12, timeout=15)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        value = cursor.fetchone()[0]
        connection.rollback()
        if int(value) != 1:
            raise RuntimeError("Unexpected pymssql SELECT result")
        return {"select_one": True, "writes": 0}
    finally:
        connection.close()


def check_browser() -> dict[str, Any]:
    Path(os.environ.get("HOME", "/tmp/home")).mkdir(parents=True, exist_ok=True)
    options = Options()
    options.binary_location = os.environ.get("CHROME_BIN", "/usr/bin/google-chrome")
    for argument in (
        "--headless=new",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--window-size=1280,900",
    ):
        options.add_argument(argument)
    service = Service(os.environ.get("CHROMEDRIVER", "/usr/bin/chromedriver"))
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.set_page_load_timeout(20)
        driver.get("data:text/html,<title>dealviewer-preflight</title>")
        if driver.title != "dealviewer-preflight":
            raise RuntimeError("Headless Chrome did not load the validation page")
        version = str(driver.capabilities.get("browserVersion", ""))
        return {"headless": True, "browser_major": version.split(".", 1)[0]}
    finally:
        driver.quit()


def _run(name: str, function: Callable[[], dict[str, Any]]) -> CheckResult:
    started = time.monotonic()
    try:
        detail = function()
        return CheckResult(name, True, int((time.monotonic() - started) * 1000), detail)
    except Exception as exc:
        detail: dict[str, Any] = {"error_type": type(exc).__name__}
        if isinstance(exc, pyodbc.Error) and exc.args:
            state = str(exc.args[0])
            if len(state) == 5 and state.isalnum():
                detail["sqlstate"] = state
        if isinstance(exc, pymssql.Error):
            numeric_codes: list[int] = []
            pending = list(exc.args)
            while pending:
                value = pending.pop(0)
                if isinstance(value, int):
                    numeric_codes.append(value)
                elif isinstance(value, (tuple, list)):
                    pending.extend(value)
            if numeric_codes:
                detail["db_codes"] = sorted(set(numeric_codes))[:5]
        if isinstance(exc, UnicodeDecodeError):
            detail["encoding"] = exc.encoding
        return CheckResult(
            name,
            False,
            int((time.monotonic() - started) * 1000),
            detail,
        )


def main() -> int:
    expected = {
        "FTP_PRIMARY_HOST",
        "FTP_PRIMARY_PASSWORD",
        "FTP_PRIMARY_PORT",
        "FTP_PRIMARY_USER",
        "FTP_SECONDARY_HOST",
        "FTP_SECONDARY_PASSWORD",
        "FTP_SECONDARY_PORT",
        "FTP_SECONDARY_USER",
        "SQL_DATABASE",
        "SQL_HOST",
        "SQL_PORT",
        "SQL_ODBC_FXWJ",
        "SQL_ODBC_FXJG",
        "SQL_ODBC_STBG",
        "SQL_ODBC_ABN_PRODUCTS",
        "SQL_ODBC_ABN_REPORTS",
        "SQL_PASSWORD",
        "SQL_USER",
    }
    configured = set(configured_secret_names())
    checks = [
        _run("secrets_schema", lambda: {"required_keys": len(expected)} if expected <= configured else (_ for _ in ()).throw(RuntimeError("Missing secret keys"))),
        _run("chinabond_issuance", lambda: check_chinabond("发行文件")),
        _run("chinabond_issuance_results", lambda: check_chinabond("发行结果")),
        _run("chinabond_trustee", lambda: check_chinabond("付息兑付与行权公告")),
        _run("chinamoney_abn", check_chinamoney),
        _run("ftp_primary_read_only", check_ftp_primary),
        _run("ftp_secondary_read_only", check_ftp_secondary),
        _run("sql_odbc_fxwj_read_only", check_odbc_fxwj),
        _run("sql_odbc_fxjg_read_only", check_odbc_fxjg),
        _run("sql_odbc_stbg_read_only", check_odbc_stbg),
        _run("sql_odbc_abn_products_read_only", check_odbc_abn_products),
        _run("sql_odbc_abn_reports_read_only", check_odbc_abn_reports),
        _run("headless_chrome", check_browser),
    ]
    payload = {
        "ok": all(check.ok for check in checks),
        "proxy_configured": bool(os.environ.get("DEALVIEWER_PROXY_URL", "").strip()),
        "checks": [asdict(check) for check in checks],
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
