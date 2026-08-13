from __future__ import annotations

import argparse
import ast
import hashlib
import ipaddress
import json
import os
import shutil
from pathlib import Path
from typing import Any, Iterable


EXPECTED_SHA256 = {
    "fxwj2023_new.py": "3395629ead75d21f96f92c40372581431b014b096dd3998afbcaf53845b2b2d7",
    "stbg_2025.py": "46e5785589c534cbda8bde09e696692a1ad350b4fb32b83165155c3faa0309b3",
    "ftp_session_utils.py": "5864c371bed7c497da1fabb60993306cfe496ade6513c9d616e36ff23471597c",
    "trust_code_utils.py": "3a3cacde4968e82dbff41a4785c82a7ec5681c70e1b97ff8ebc20bf2416dd2cf",
}

SCRIPT_NAMES = ("fxwj2023_new.py", "stbg_2025.py")
HELPER_NAMES = ("ftp_session_utils.py", "trust_code_utils.py")

MODULE_SECRET_MAP = {
    "FTP_HOST": ("FTP_PRIMARY_HOST", "secret"),
    "FTP_PORT": ("FTP_PRIMARY_PORT", "secret_int"),
    "FTP_USER": ("FTP_PRIMARY_USER", "secret"),
    "FTP_PASS": ("FTP_PRIMARY_PASSWORD", "secret"),
    "FTP2_HOST": ("FTP_SECONDARY_HOST", "secret"),
    "FTP2_PORT": ("FTP_SECONDARY_PORT", "secret_int"),
    "FTP2_USER": ("FTP_SECONDARY_USER", "secret"),
    "FTP2_PASS": ("FTP_SECONDARY_PASSWORD", "secret"),
}

DIRECT_TEST_FUNCTION = '''def test_configured_proxy(proxies):
    mode = "configured proxy" if proxies else "direct connection"
    print(f"testing {mode}")
    test_url = "https://www.chinabond.com.cn/cbiw/trs/getContentByConditions"
    test_payload = {
        "parentChnlName": "zqzl_zjzzczj",
        "excludeParentChnlNames": [],
        "childChnlDesc": "发行文件",
        "hasAppendix": True,
        "siteName": "chinaBond",
        "pageSize": 50,
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
    test_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "User-Agent": "DealViewer-R760/1.0",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/",
    }
    request_kwargs = {"proxies": proxies} if proxies else {}
    if proxies:
        print(f"proxy: {mask_proxy_url(proxies.get('https'))}")
    try:
        response = requests.post(
            test_url,
            json=test_payload,
            headers=test_headers,
            timeout=(10, 40),
            **request_kwargs,
        )
        if response.status_code != 200:
            print(f"{mode} test returned HTTP {response.status_code}")
            return False
        response_data = response.json()
        if not response_data.get("success"):
            print(f"{mode} test returned an unsuccessful business response")
            return False
        item_count = len(response_data.get("data", {}).get("list", []))
        print(f"{mode} test succeeded against chinabond API. items={item_count}")
        return item_count > 0
    except Exception as exc:
        print(f"{mode} test failed: {type(exc).__name__}")
        return False
'''


RESILIENT_FTP_LIST_FUNCTION = '''def list_ftp_directory_with_retry(ftp, path, retries=5):
    """List an FTP directory and rebuild a desynchronised control connection."""
    last_error = None
    for attempt in range(retries):
        try:
            return list_ftp_directory(ftp, path)
        except ftplib.all_errors as exc:
            last_error = exc
            remaining = retries - attempt - 1
            print(
                f"FTP directory list failed for {path}: {type(exc).__name__}; "
                f"remaining_retries={remaining}"
            )
            if remaining <= 0:
                break
            try:
                reconnect_ftp_connection(ftp, timeout=120)
                print("FTP control connection rebuilt after directory-list failure")
            except Exception as reconnect_error:
                print(
                    "FTP reconnect after directory-list failure failed: "
                    f"{type(reconnect_error).__name__}"
                )
            wait_time = min(5 * (2 ** attempt), 60)
            print(f"Waiting {wait_time} seconds before directory-list retry")
            time.sleep(wait_time)
    raise RuntimeError(
        f"Failed to list FTP directory {path} after {retries} attempts"
    ) from last_error
'''


RESILIENT_FTP_UPLOAD_FUNCTION = '''def upload_file_to_ftp_with_retry(
    ftp, local_file_path, ftp_folder, ftp_file_path, file_name, retries=5
):
    """Upload idempotently and reconnect after every FTP protocol/network error."""
    last_error = None
    for attempt in range(retries):
        try:
            dir_contents = list_ftp_directory_with_retry(ftp, ftp_folder)
            if file_name in dir_contents:
                print(f"File already exists on FTP in folder: {ftp_folder}")
                return True

            print(
                f"Writing {local_file_path} to {ftp_file_path} "
                f"=====> (Attempt {attempt + 1}/{retries})"
            )
            with open(local_file_path, "rb") as handle:
                with ftp_operation(ftp):
                    ftp.storbinary(f"STOR {ftp_file_path}", handle)
            print(f"Successfully uploaded {ftp_file_path}")
            return True
        except ftplib.all_errors as exc:
            last_error = exc
            remaining = retries - attempt - 1
            print(
                f"FTP upload failed for {ftp_file_path}: {type(exc).__name__}; "
                f"remaining_retries={remaining}"
            )
            if remaining <= 0:
                break
            try:
                reconnect_ftp_connection(ftp, timeout=120)
                print("FTP control connection rebuilt after upload failure")
            except Exception as reconnect_error:
                print(
                    "FTP reconnect after upload failure failed: "
                    f"{type(reconnect_error).__name__}"
                )
            wait_time = min(5 * (2 ** attempt), 60)
            print(f"Waiting {wait_time} seconds before upload retry")
            time.sleep(wait_time)
    raise RuntimeError(
        f"Failed to upload FTP file {ftp_file_path} after {retries} attempts"
    ) from last_error
'''


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assignment_name(node: ast.AST) -> str | None:
    if not isinstance(node, (ast.Assign, ast.AnnAssign)):
        return None
    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
    if len(targets) == 1 and isinstance(targets[0], ast.Name):
        return targets[0].id
    return None


def _literal(node: ast.AST, description: str) -> Any:
    try:
        return ast.literal_eval(node)
    except Exception as exc:
        raise RuntimeError(f"Expected a literal for {description}") from exc


def _find_function(tree: ast.Module, name: str) -> ast.FunctionDef:
    matches = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name]
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly one function named {name}")
    return matches[0]


def _is_library_connect(call: ast.Call, library: str) -> bool:
    function = call.func
    return (
        isinstance(function, ast.Attribute)
        and function.attr == "connect"
        and isinstance(function.value, ast.Name)
        and function.value.id == library
    )


def _string_literals(node: ast.AST) -> Iterable[str]:
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            yield child.value


class SourceEditor:
    def __init__(self, source: str):
        self.source = source
        self.encoded = source.encode("utf-8")
        self.line_offsets: list[int] = []
        offset = 0
        for line in self.encoded.splitlines(keepends=True):
            self.line_offsets.append(offset)
            offset += len(line)
        if not self.line_offsets:
            self.line_offsets.append(0)
        self.replacements: list[tuple[int, int, bytes, str]] = []

    def bounds(self, node: ast.AST) -> tuple[int, int]:
        if not all(hasattr(node, name) for name in ("lineno", "col_offset", "end_lineno", "end_col_offset")):
            raise RuntimeError("AST node does not have complete source coordinates")
        start = self.line_offsets[node.lineno - 1] + node.col_offset
        end = self.line_offsets[node.end_lineno - 1] + node.end_col_offset
        return start, end

    def replace_node(self, node: ast.AST, replacement: str, label: str) -> None:
        start, end = self.bounds(node)
        self.replacements.append((start, end, replacement.encode("utf-8"), label))

    def replace_range(self, first: ast.AST, last: ast.AST, replacement: str, label: str) -> None:
        start, _ = self.bounds(first)
        _, end = self.bounds(last)
        self.replacements.append((start, end, replacement.encode("utf-8"), label))

    def insert_start(self, text: str, label: str) -> None:
        self.replacements.append((0, 0, text.encode("utf-8"), label))

    def render(self) -> str:
        ordered = sorted(self.replacements, key=lambda item: (item[0], item[1]))
        previous_end = -1
        for start, end, _, label in ordered:
            if start < previous_end:
                raise RuntimeError(f"Overlapping source replacement: {label}")
            previous_end = max(previous_end, end)
        output = self.encoded
        for start, end, replacement, _ in reversed(ordered):
            output = output[:start] + replacement + output[end:]
        return output.decode("utf-8")


class BundleBuilder:
    def __init__(self, raw_dir: Path, output_dir: Path, secrets_path: Path):
        self.raw_dir = raw_dir
        self.output_dir = output_dir
        self.secrets_path = secrets_path
        self.secrets: dict[str, Any] = {}
        self.banned_values: set[str] = set()
        self.source_hashes: dict[str, str] = {}

    def record_secret(self, key: str, value: Any) -> None:
        if not isinstance(value, (str, int)):
            raise RuntimeError(f"Secret {key} has an unsupported type")
        if key in self.secrets and self.secrets[key] != value:
            raise RuntimeError(f"Conflicting source values for secret {key}")
        self.secrets[key] = value
        if (
            isinstance(value, str)
            and len(value) >= 8
            and ("PASSWORD" in key or key.startswith("SQL_ODBC"))
        ):
            self.banned_values.add(value)

    def verify_inputs(self) -> None:
        for name, expected_hash in EXPECTED_SHA256.items():
            path = self.raw_dir / name
            if not path.is_file():
                raise RuntimeError(f"Required authoritative source is missing: {name}")
            actual_hash = _sha256(path)
            if actual_hash != expected_hash:
                raise RuntimeError(f"Authoritative source hash mismatch: {name}")
            self.source_hashes[name] = actual_hash

    def transform_script(self, name: str) -> str:
        source = (self.raw_dir / name).read_text(encoding="utf-8-sig")
        tree = ast.parse(source, filename=name)
        editor = SourceEditor(source)
        editor.insert_start(
            "from dealviewer_runtime import pymssql_connection_kwargs, secret, secret_int\n",
            "runtime import",
        )

        module_assignments: dict[str, ast.Assign | ast.AnnAssign] = {}
        for node in tree.body:
            target = _assignment_name(node)
            if target:
                module_assignments.setdefault(target, node)

        for variable, (secret_name, loader) in MODULE_SECRET_MAP.items():
            node = module_assignments.get(variable)
            if node is None:
                raise RuntimeError(f"{name} is missing module assignment {variable}")
            self.record_secret(secret_name, _literal(node.value, f"{name}:{variable}"))
            editor.replace_node(node.value, f'{loader}("{secret_name}")', f"{name}:{variable}")

        sql_key = "SQL_ODBC_FXWJ" if name == "fxwj2023_new.py" else "SQL_ODBC_STBG"
        sql_function = _find_function(tree, "get_sql_connection")
        sql_assignments = [
            node
            for node in ast.walk(sql_function)
            if isinstance(node, (ast.Assign, ast.AnnAssign)) and _assignment_name(node) == "conn_str"
        ]
        if len(sql_assignments) != 1:
            raise RuntimeError(f"{name} must have exactly one conn_str assignment")
        sql_assignment = sql_assignments[0]
        self.record_secret(sql_key, _literal(sql_assignment.value, f"{name}:conn_str"))
        editor.replace_node(sql_assignment.value, f'secret("{sql_key}")', f"{name}:conn_str")

        insert_function = _find_function(tree, "insert")
        pymssql_calls = [
            node
            for node in ast.walk(insert_function)
            if isinstance(node, ast.Call) and _is_library_connect(node, "pymssql")
        ]
        if len(pymssql_calls) != 1:
            raise RuntimeError(f"{name} must have exactly one pymssql connection in insert")
        pymssql_call = pymssql_calls[0]
        keyword_values = {item.arg: _literal(item.value, f"{name}:pymssql:{item.arg}") for item in pymssql_call.keywords}
        host_value = keyword_values.get("host", keyword_values.get("server"))
        for secret_name, value in (
            ("SQL_LEGACY_HOST", host_value),
            ("SQL_USER", keyword_values.get("user")),
            ("SQL_PASSWORD", keyword_values.get("password")),
            ("SQL_DATABASE", keyword_values.get("database")),
        ):
            if value is None:
                raise RuntimeError(f"{name} is missing pymssql value {secret_name}")
            self.record_secret(secret_name, value)
        editor.replace_node(
            pymssql_call,
            f'pyodbc.connect(secret("{sql_key}"))',
            f"{name}:pymssql.connect",
        )

        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)) or _assignment_name(node) != "proxies":
                continue
            if getattr(node, "col_offset", 0) == 0:
                continue
            if not any(isinstance(child, ast.Name) and child.id == "proxy_url" for child in ast.walk(node.value)):
                continue
            editor.replace_node(
                node.value,
                '({"http": proxy_url, "https": proxy_url} if proxy_url else {})',
                f"{name}:optional function proxy",
            )

        if name == "fxwj2023_new.py":
            list_retry_function = _find_function(tree, "list_ftp_directory_with_retry")
            editor.replace_node(
                list_retry_function,
                RESILIENT_FTP_LIST_FUNCTION.rstrip(),
                "fxwj:reconnect all FTP list errors",
            )
            upload_retry_function = _find_function(tree, "upload_file_to_ftp_with_retry")
            editor.replace_node(
                upload_retry_function,
                RESILIENT_FTP_UPLOAD_FUNCTION.rstrip(),
                "fxwj:reconnect all FTP upload errors",
            )
            file_handlers = [
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "logging"
                and node.func.attr == "FileHandler"
                and node.args
            ]
            if len(file_handlers) != 1:
                raise RuntimeError("fxwj source must have exactly one logging.FileHandler")
            editor.replace_node(
                file_handlers[0].args[0],
                'os.environ.get("DEALVIEWER_LEGACY_LOG_FILE", "/logs/fxwj-legacy.log")',
                "fxwj:writable legacy log",
            )
            default_proxy = module_assignments.get("default_proxy_string")
            if default_proxy is None:
                raise RuntimeError("fxwj source is missing default_proxy_string")
            for value in _string_literals(default_proxy.value):
                if len(value) >= 8:
                    self.banned_values.add(value)
            editor.replace_node(default_proxy.value, '""', "fxwj:direct proxy default")
            proxy_test = _find_function(tree, "test_configured_proxy")
            editor.replace_node(proxy_test, DIRECT_TEST_FUNCTION.rstrip(), "fxwj:direct connectivity test")

            mail_function = _find_function(tree, "mail")
            password_assignments = [
                node
                for node in ast.walk(mail_function)
                if isinstance(node, (ast.Assign, ast.AnnAssign)) and _assignment_name(node) == "passwd"
            ]
            if len(password_assignments) != 1:
                raise RuntimeError("fxwj mail function must have exactly one passwd assignment")
            password_assignment = password_assignments[0]
            self.record_secret("SMTP_PASSWORD", _literal(password_assignment.value, "fxwj:mail password"))
            editor.replace_node(password_assignment.value, 'secret("SMTP_PASSWORD")', "fxwj:SMTP password")
        else:
            proxy_start = module_assignments.get("proxy_string")
            proxy_end = module_assignments.get("proxies")
            if proxy_start is None or proxy_end is None or proxy_end.lineno <= proxy_start.lineno:
                raise RuntimeError("stbg proxy configuration block was not found")
            for value in _string_literals(proxy_start.value):
                if len(value) >= 8:
                    self.banned_values.add(value)
            editor.replace_range(
                proxy_start,
                proxy_end,
                'proxy_url = os.environ.get("DEALVIEWER_PROXY_URL", "").strip() or None\n'
                'proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else {}',
                "stbg:optional proxy block",
            )

            upload_function = _find_function(tree, "upload_file")
            legacy_map = {
                "host": ("FTP_LEGACY_HOST", "secret"),
                "port": ("FTP_LEGACY_PORT", "secret_int"),
                "username": ("FTP_LEGACY_USER", "secret"),
                "password": ("FTP_LEGACY_PASSWORD", "secret"),
            }
            found: set[str] = set()
            for node in ast.walk(upload_function):
                if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                    continue
                variable = _assignment_name(node)
                if variable not in legacy_map:
                    continue
                secret_name, loader = legacy_map[variable]
                self.record_secret(secret_name, _literal(node.value, f"stbg:upload_file:{variable}"))
                editor.replace_node(node.value, f'{loader}("{secret_name}")', f"stbg:legacy FTP {variable}")
                found.add(variable)
            if found != set(legacy_map):
                raise RuntimeError("stbg legacy FTP assignments are incomplete")

        output = editor.render()
        parsed_output = ast.parse(output, filename=name)
        for literal in _string_literals(parsed_output):
            for value in self.banned_values:
                if value and value in literal:
                    fingerprint = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
                    raise RuntimeError(
                        f"An active sensitive literal remains in transformed {name}: sha256={fingerprint}"
                    )
        # Legacy files also contained disabled connection examples in comments.
        # Redact those after proving that no executable string literal still
        # contains a captured credential.
        for value in sorted(self.banned_values, key=len, reverse=True):
            if value:
                output = output.replace(value, "<redacted>")
        output = output.replace("ProxyJet", "configured proxy").replace(
            "proxyjet", "configured proxy"
        )
        compile(output, name, "exec")
        return output

    def derive_public_sql_route(self) -> None:
        connection_string = str(self.secrets["SQL_ODBC_FXWJ"])
        fields = {
            part.split("=", 1)[0].strip().upper(): part.split("=", 1)[1].strip()
            for part in connection_string.split(";")
            if "=" in part
        }
        raw_server = fields.get("SERVER", "").removeprefix("tcp:")
        if "," in raw_server:
            host, raw_port = raw_server.rsplit(",", 1)
        elif raw_server.count(":") == 1:
            host, raw_port = raw_server.rsplit(":", 1)
        else:
            host, raw_port = raw_server, "1433"
        host = host.strip()
        try:
            port = int(raw_port.strip())
        except ValueError as exc:
            raise RuntimeError("ODBC SERVER contains an invalid port") from exc
        if not host or not 1 <= port <= 65535:
            raise RuntimeError("ODBC SERVER contains an invalid public route")
        try:
            if ipaddress.ip_address(host).is_private:
                raise RuntimeError("ODBC SERVER still points to a private address")
        except ValueError:
            pass
        expected_pairs = {
            "UID": "SQL_USER",
            "PWD": "SQL_PASSWORD",
            "DATABASE": "SQL_DATABASE",
        }
        for field_name, secret_name in expected_pairs.items():
            if fields.get(field_name) != str(self.secrets.get(secret_name, "")):
                raise RuntimeError(f"ODBC and pymssql credentials differ for {field_name}")
        self.record_secret("SQL_HOST", host)
        self.record_secret("SQL_PORT", port)

    def write_secrets(self) -> None:
        self.secrets_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.secrets_path.with_name(self.secrets_path.name + ".tmp")
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(self.secrets, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, self.secrets_path)
        os.chmod(self.secrets_path, 0o600)

    def build(self) -> dict[str, Any]:
        self.verify_inputs()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        transformed: dict[str, str] = {}
        for name in SCRIPT_NAMES:
            transformed[name] = self.transform_script(name)
        self.derive_public_sql_route()
        for name, source in transformed.items():
            target = self.output_dir / name
            target.write_text(source, encoding="utf-8", newline="\n")
            os.chmod(target, 0o644)
        for name in HELPER_NAMES:
            target = self.output_dir / name
            shutil.copyfile(self.raw_dir / name, target)
            os.chmod(target, 0o644)
            compile(target.read_text(encoding="utf-8-sig"), name, "exec")
        self.write_secrets()
        return {
            "ok": True,
            "sources": self.source_hashes,
            "secret_keys": sorted(self.secrets),
            "output_files": sorted((*SCRIPT_NAMES, *HELPER_NAMES)),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare a secret-free R760 DealViewer runtime bundle")
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--secrets-file", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    builder = BundleBuilder(args.raw_dir.resolve(), args.output_dir.resolve(), args.secrets_file.resolve())
    result = builder.build()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
