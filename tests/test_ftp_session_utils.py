from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "dome1" / "ftp_session_utils.py"


def load_module():
    assert MODULE_PATH.exists(), f"Missing helper module: {MODULE_PATH}"
    spec = importlib.util.spec_from_file_location("ftp_session_utils", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeFTP:
    def __init__(self) -> None:
        self.host = "example-host"
        self.port = 21
        self.encoding = "utf-8"
        self.closed = False
        self.connected = []
        self.logins = []
        self.commands = []

    def close(self) -> None:
        self.closed = True

    def connect(self, host: str, port: int, timeout: int = 0) -> None:
        self.connected.append((host, port, timeout))
        self.host = host
        self.port = port

    def login(self, user: str, passwd: str) -> None:
        self.logins.append((user, passwd))

    def voidcmd(self, command: str) -> str:
        self.commands.append(command)
        return "200 OK"


def test_reconnect_uses_attached_credentials_for_secondary_ftp() -> None:
    module = load_module()
    ftp = FakeFTP()

    module.attach_ftp_config(
        ftp,
        host="113.125.202.171",
        port=21121,
        user="gsuser",
        password="secondary-pass",
        encoding="utf-8",
    )
    module.reconnect_ftp_connection(ftp, timeout=120)

    assert ftp.connected == [("113.125.202.171", 21121, 120)]
    assert ftp.logins == [("gsuser", "secondary-pass")]


def test_reconnect_preserves_encoding_after_relogin() -> None:
    module = load_module()
    ftp = FakeFTP()
    ftp.encoding = "gbk"

    module.attach_ftp_config(
        ftp,
        host="113.125.202.171",
        port=11421,
        user="dv",
        password="primary-pass",
        encoding="gbk",
    )
    module.reconnect_ftp_connection(ftp, timeout=60)

    assert ftp.encoding == "gbk"


def test_keepalive_skips_noop_while_ftp_operation_lock_is_held() -> None:
    module = load_module()
    ftp = FakeFTP()

    module.attach_ftp_config(
        ftp,
        host="113.125.202.171",
        port=21121,
        user="gsuser",
        password="secondary-pass",
        encoding="utf-8",
    )

    with module.ftp_operation(ftp):
        assert module.try_keepalive_noop(ftp) is False

    assert ftp.commands == []


def test_keepalive_sends_noop_when_ftp_is_idle() -> None:
    module = load_module()
    ftp = FakeFTP()

    module.attach_ftp_config(
        ftp,
        host="113.125.202.171",
        port=21121,
        user="gsuser",
        password="secondary-pass",
        encoding="utf-8",
    )

    assert module.try_keepalive_noop(ftp) is True
    assert ftp.commands == ["NOOP"]
