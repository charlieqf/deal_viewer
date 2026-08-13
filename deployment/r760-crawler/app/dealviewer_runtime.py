from __future__ import annotations

import json
import os
import stat
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_SECRETS_FILE = "/run/dealviewer/secrets.json"


@lru_cache(maxsize=1)
def _load_secrets() -> dict[str, Any]:
    path = Path(os.environ.get("DEALVIEWER_SECRETS_FILE", DEFAULT_SECRETS_FILE))
    if not path.is_absolute():
        raise RuntimeError("DEALVIEWER_SECRETS_FILE must be an absolute path")
    file_stat = path.stat()
    if not stat.S_ISREG(file_stat.st_mode):
        raise RuntimeError("DealViewer secrets path is not a regular file")
    if file_stat.st_mode & 0o022:
        raise RuntimeError("DealViewer secrets file must not be group/world writable")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError("DealViewer secrets file must contain a JSON object")
    return data


def secret(name: str, *, required: bool = True) -> str:
    value = _load_secrets().get(name)
    if value is None or value == "":
        if required:
            raise RuntimeError(f"Required DealViewer secret is missing: {name}")
        return ""
    if not isinstance(value, (str, int)):
        raise RuntimeError(f"DealViewer secret has an invalid type: {name}")
    return str(value)


def secret_int(name: str) -> int:
    value = secret(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"DealViewer secret is not an integer: {name}") from exc


def pymssql_connection_kwargs() -> dict[str, Any]:
    return {
        "server": secret("SQL_HOST"),
        "port": secret_int("SQL_PORT"),
        "user": secret("SQL_USER"),
        "password": secret("SQL_PASSWORD"),
        "database": secret("SQL_DATABASE"),
        "charset": "utf8",
    }


def configured_secret_names() -> list[str]:
    return sorted(_load_secrets())
