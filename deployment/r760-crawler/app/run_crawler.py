from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


APP_DIR = Path(__file__).resolve().parent
STATE_DIR = Path(os.environ.get("DEALVIEWER_STATE_DIR", "/state"))
LOG_DIR = Path(os.environ.get("DEALVIEWER_LOG_DIR", "/logs"))
ALLOWED_TASKS = {
    "preflight",
    "syntax",
    "abn-products",
    "abn-reports",
    "fxwj",
    "stbg",
    "stbg-page1",
}


def _redact_child_line(line: str) -> str:
    stripped = line.lstrip()
    if (
        stripped.startswith("data = {'sign':")
        or "Cookie:" in line
        or "AlteonP10" in line
        or ("sign=" in line and "infoLevel=" in line)
    ):
        return "[redacted transient ChinaMoney authentication material]\n"
    return line


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _acquire_lock():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    handle = (STATE_DIR / "container-run.lock").open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError("Another DealViewer crawler is already running") from exc
    return handle


def _run_child(
    task: str,
    script_name: str,
    extra_env: dict[str, str] | None = None,
    work_subdir: str | None = None,
) -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    started = _timestamp()
    log_path = LOG_DIR / f"{task}-{started}.log"
    status_path = LOG_DIR / f"{task}-{started}.status.json"
    environment = os.environ.copy()
    environment.update(
        {
            "DEALVIEWER_PROXY_URL": environment.get("DEALVIEWER_PROXY_URL", ""),
            "DEALVIEWER_BROWSER_WARMUP": "0",
            "STBG_BROWSER_WARMUP": "0",
            "PYTHONUNBUFFERED": "1",
        }
    )
    if extra_env:
        environment.update(extra_env)
    command = [sys.executable, str(APP_DIR / script_name)]
    working_dir = APP_DIR
    if work_subdir:
        working_dir = STATE_DIR / "work" / work_subdir
        working_dir.mkdir(parents=True, exist_ok=True)
    exit_code = 1
    with log_path.open("w", encoding="utf-8", errors="replace") as log:
        log.write(f"task={task} started_utc={started} script={script_name}\n")
        log.flush()
        process = subprocess.Popen(
            command,
            cwd=working_dir,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            safe_line = _redact_child_line(line)
            sys.stdout.write(safe_line)
            sys.stdout.flush()
            log.write(safe_line)
            log.flush()
        exit_code = process.wait()
        log.write(f"exit_code={exit_code}\n")
    payload = {
        "task": task,
        "script": script_name,
        "started_utc": started,
        "ended_utc": _timestamp(),
        "exit_code": exit_code,
        "log_file": log_path.name,
    }
    temporary = status_path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, status_path)
    return exit_code


def _syntax_check() -> int:
    files = (
        "ABN2025_products_new.py",
        "ABN2025_new.py",
        "abn_offer_type_utils.py",
        "fxwj2023_new.py",
        "stbg_2025.py",
        "dealviewer_runtime.py",
        "preflight.py",
        "run_crawler.py",
    )
    for filename in files:
        source = (APP_DIR / filename).read_text(encoding="utf-8")
        compile(source, filename, "exec")
    print(json.dumps({"ok": True, "compiled": list(files)}, sort_keys=True))
    return 0


def _run_stbg(page_one_only: bool) -> int:
    if page_one_only:
        pages = [1]
    else:
        raw_pages = os.environ.get("STBG_PAGES", "6,5,4,3,2,1")
        pages = [int(value.strip()) for value in raw_pages.split(",") if value.strip()]
    if not pages or any(page < 1 for page in pages):
        raise RuntimeError("STBG_PAGES must contain positive page numbers")
    for page in pages:
        code = _run_child(
            f"stbg-page{page}",
            "stbg_2025.py",
            {
                "STBG_PAGE_NUM": str(page),
                "STBG_WRITE_UPDATE_LOG": "auto",
                "STBG_BROWSER_WARMUP": "0",
            },
        )
        if code != 0:
            return code
    return 0


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    task = args[0] if args else "preflight"
    if task not in ALLOWED_TASKS or len(args) != 1:
        print(
            "usage: run_crawler.py "
            "preflight|syntax|abn-products|abn-reports|fxwj|stbg|stbg-page1",
            file=sys.stderr,
        )
        return 2
    lock_handle = _acquire_lock()
    try:
        if task == "preflight":
            from preflight import main as preflight_main

            return preflight_main()
        if task == "syntax":
            return _syntax_check()
        if task == "abn-products":
            return _run_child(
                "abn-products",
                "ABN2025_products_new.py",
                {"ABN_FTP_KEEPALIVE": "0"},
                work_subdir="abn-products",
            )
        if task == "abn-reports":
            return _run_child(
                "abn-reports",
                "ABN2025_new.py",
                {"ABN_FTP_KEEPALIVE": "0"},
                work_subdir="abn-reports",
            )
        if task == "fxwj":
            return _run_child("fxwj", "fxwj2023_new.py")
        return _run_stbg(page_one_only=task == "stbg-page1")
    finally:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
