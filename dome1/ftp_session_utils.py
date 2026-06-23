from __future__ import annotations

from contextlib import contextmanager
import threading


def attach_ftp_config(
    ftp,
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    encoding: str | None = None,
    enable_utf8: bool = False,
) -> None:
    ftp._opencode_host = host
    ftp._opencode_port = port
    ftp._opencode_user = user
    ftp._opencode_password = password
    ftp._opencode_encoding = encoding
    ftp._opencode_enable_utf8 = enable_utf8
    if not hasattr(ftp, "_opencode_lock"):
        ftp._opencode_lock = threading.RLock()
    if not hasattr(ftp, "_opencode_active_ops"):
        ftp._opencode_active_ops = 0


@contextmanager
def ftp_operation(ftp):
    lock = getattr(ftp, "_opencode_lock")
    with lock:
        ftp._opencode_active_ops += 1
        try:
            yield
        finally:
            ftp._opencode_active_ops -= 1


def try_keepalive_noop(ftp) -> bool:
    lock = getattr(ftp, "_opencode_lock")
    if not lock.acquire(blocking=False):
        return False
    try:
        if getattr(ftp, "_opencode_active_ops", 0) > 0:
            return False
        ftp.voidcmd("NOOP")
        return True
    finally:
        lock.release()


def reconnect_ftp_connection(ftp, *, timeout: int) -> None:
    with ftp_operation(ftp):
        host = getattr(ftp, "_opencode_host")
        port = getattr(ftp, "_opencode_port")
        user = getattr(ftp, "_opencode_user")
        password = getattr(ftp, "_opencode_password")
        encoding = getattr(ftp, "_opencode_encoding", None)
        enable_utf8 = getattr(ftp, "_opencode_enable_utf8", False)

        try:
            ftp.close()
        except Exception:
            pass

        ftp.connect(host, port, timeout=timeout)
        ftp.login(user, password)
        if enable_utf8:
            ftp.sendcmd("OPTS UTF8 ON")
        if encoding:
            ftp.encoding = encoding
