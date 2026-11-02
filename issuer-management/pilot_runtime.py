"""Issuer runtime with explicit pilot/production ownership and database guards."""
from __future__ import annotations

import argparse
from datetime import date
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import sys

DATABASE = "DealViewerIssuerPilot_20261020"
PURPOSE = "DealViewer issuer SPDB pilot 2026-10-20"
ROOT = Path(os.environ.get("ISSUER_PILOT_ROOT", "/pilot"))
CONFIG = json.loads((ROOT / 'run-config.json').read_text(encoding='utf-8')) if (ROOT / 'run-config.json').exists() else {}
PRODUCTION = CONFIG.get('database') == 'PortfolioManagement'
if CONFIG.get('database') not in (None, DATABASE, 'PortfolioManagement'):
    raise RuntimeError('Unapproved issuer database')
if PRODUCTION:
    DATABASE = 'PortfolioManagement'
    PURPOSE = 'DealViewer issuer management production v1'
ISSUER = CONFIG.get('issuer', "上海浦东发展银行股份有限公司")
DISPLAY_NAME = CONFIG.get('display_name', '浦发银行')
SOURCE = ROOT / "input" / "source"
ARTIFACTS = ROOT / "artifacts"
METRICS = ["total_assets", "capital_adequacy_ratio", "nonperforming_loan_ratio", "provision_coverage_ratio", "operating_revenue", "total_profit"]
if CONFIG.get('metrics_version') == 'profit-v2':
    METRICS += ['net_profit', 'net_profit_attributable_to_parent']
PIPELINE = CONFIG.get('pipeline', "spdb-pilot-v1")
if PIPELINE=='issuer-production-v1' and not PRODUCTION:
    raise RuntimeError('Production pipeline requires explicit production database configuration')


def sha(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def encode(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")


def atomic_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".part")
    temp.write_bytes(encode(data))
    temp.replace(path)


def guarded_connection(autocommit: bool = False, bootstrap: bool = False):
    import pyodbc
    from dealviewer_runtime import secret
    cn = pyodbc.connect(secret("SQL_ODBC_ABN_PRODUCTS"), timeout=15, autocommit=autocommit)
    cn.timeout = 45
    row = cn.execute("SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ServerName')), CONVERT(nvarchar(128),CONNECTIONPROPERTY('local_net_address')), CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion'))").fetchone()
    if row[0] != "CNGY_6_6143_DEA\\MSSQL" or row[1] != "172.16.6.143" or not row[2].startswith("12."):
        cn.close()
        raise RuntimeError("Pilot target instance guard rejected connection")
    if not bootstrap:
        cn.execute("USE [" + DATABASE + "]")
        marker = 'IssuerManagementPurpose' if PRODUCTION else 'IssuerPilotPurpose'
        row = cn.execute("SELECT CONVERT(nvarchar(256),value) FROM sys.extended_properties WHERE class=0 AND name=?", marker).fetchone()
        if not row or row[0] != PURPOSE:
            cn.close()
            raise RuntimeError("Pilot database ownership marker missing")
    return cn


def bootstrap() -> None:
    if PRODUCTION:
        raise RuntimeError('Production requires production_migrate.py, never pilot bootstrap')
    cn = guarded_connection(autocommit=True, bootstrap=True)
    if not cn.execute("SELECT DB_ID(?)", DATABASE).fetchval():
        paths = cn.execute("SELECT CONVERT(nvarchar(512),SERVERPROPERTY('InstanceDefaultDataPath')),CONVERT(nvarchar(512),SERVERPROPERTY('InstanceDefaultLogPath'))").fetchone()
        if any(not p or "'" in p for p in paths):
            raise RuntimeError("Invalid instance storage path")
        data_path, log_path = (p.rstrip("\\") + "\\" + DATABASE + suffix for p, suffix in zip(paths, [".mdf", ".ldf"]))
        cn.execute("CREATE DATABASE [" + DATABASE + "] ON PRIMARY (NAME=N'IssuerPilotData',FILENAME=N'" + data_path + "',SIZE=64MB,MAXSIZE=1024MB,FILEGROWTH=64MB) LOG ON (NAME=N'IssuerPilotLog',FILENAME=N'" + log_path + "',SIZE=32MB,MAXSIZE=512MB,FILEGROWTH=32MB)")
        cn.execute("ALTER DATABASE [" + DATABASE + "] SET RECOVERY SIMPLE")
        cn.execute("USE [" + DATABASE + "]")
        cn.execute("EXEC sys.sp_addextendedproperty @name=N'IssuerPilotPurpose', @value=?", PURPOSE)
    cn.close()
    cn = guarded_connection()
    schema = (Path(__file__).parent / "pilot_schema.sql").read_bytes()
    schema_hash = hashlib.sha256(schema).hexdigest()
    previous = cn.execute("SELECT CONVERT(nvarchar(128),value) FROM sys.extended_properties WHERE class=0 AND name='IssuerPilotSchemaHash'").fetchone()
    if previous:
        if previous[0] != schema_hash:
            raise RuntimeError("Schema changed after migration; explicit additive migration required")
    else:
        for batch in re.split(r"(?m)^GO\s*$", schema.decode("utf-8")):
            if batch.strip():
                cn.execute(batch)
        cn.execute("EXEC sys.sp_addextendedproperty @name=N'IssuerPilotSchemaHash', @value=?", schema_hash)
        cn.commit()
    for batch in re.split(r"(?m)^GO\s*$", (Path(__file__).parent/'pilot_migration_002_storage.sql').read_text(encoding='utf-8')):
        if batch.strip():cn.execute(batch)
    cn.commit()
    tables = [r[0] for r in cn.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('DV') ORDER BY name")]
    assert len(tables) == 15
    cn.close()
    atomic_json(ARTIFACTS / "database-bootstrap.json", {"database": DATABASE, "schema_sha256": schema_hash, "tables": tables, "purpose": PURPOSE})
    print(json.dumps({"event": "database_ready", "database": DATABASE, "tables": len(tables)}), flush=True)


def period_from_name(name: str) -> tuple[str, str, str]:
    match = re.search(r"20\d\d", name)
    if not match:raise ValueError('Report year not present in title')
    year = int(match.group())
    if any(t in name for t in ('半年', '中期', '二季度')): end = '06-30'
    elif '三季' in name: end = '09-30'
    elif '一季' in name: end = '03-31'
    elif '年度' in name or re.search(r'经审计的?财务', name): end = '12-31'
    else: raise ValueError('Unrecognized report period: '+name)
    kind = "audit_report" if "审计" in name else "annual_report" if end == "12-31" else "semiannual_report" if end == "06-30" else "quarterly_report"
    return f"{year}-01-01", f"{year}-{end}", kind


def register(local=False) -> None:
    from source_quarantine import quarantined
    manifest = json.loads((ROOT / ("input/local-source-manifest.json" if local else "input/prepared/manifest.json")).read_text(encoding="utf-8"))
    archive = None
    if PRODUCTION:
        archive=json.loads((ROOT/'input/archive-receipt.json').read_text(encoding='utf-8'))
        if archive.get('backend') not in ('dealviewer_filesystem','r760_filesystem') or not archive.get('verified_server'):
            raise RuntimeError('Production original archive has not been verified')
        for item in manifest:
            receipt=archive['files'][item['sha256']]
            if receipt['sha256']!=item['sha256'] or receipt['bytes']!=item['bytes'] or receipt['storage_key']!=item['sha256']+'.pdf':
                raise RuntimeError('Production archive receipt mismatch')
    cn = guarded_connection()
    cn.execute("DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=?,@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=10000; IF @r<0 THROW 51000,'Issuer registration lock unavailable',1;", 'issuer-register:'+ISSUER)
    issuer_id = cn.execute("SELECT IssuerId FROM DV.Issuer WHERE LegalName=?", ISSUER).fetchval()
    if issuer_id and cn.execute('SELECT MonitoringStatus FROM DV.Issuer WHERE IssuerId=?', issuer_id).fetchval() != 'active':
        cn.close()
        raise RuntimeError('Issuer monitoring paused; supplementation prohibited')
    if PRODUCTION and ('货币网无' in CONFIG.get('source_folder','') or any('货币网无' in x.get('source_relative', '') for x in manifest)):
        cn.close()
        raise RuntimeError('China Money source unavailable; registration prohibited')
    if not issuer_id:
        issuer_id = cn.execute("INSERT DV.Issuer(LegalName,DisplayName,InstitutionType) OUTPUT inserted.IssuerId VALUES(?,?,?)", ISSUER, DISPLAY_NAME, CONFIG.get('institution_type','bank')).fetchval()
        cn.execute("INSERT DV.IssuerAlias(IssuerId,AliasName,NormalizedName,NormalizationVersion,SourceSystem,ReviewStatus) VALUES(?,?,?,?,?,?)", issuer_id, DISPLAY_NAME, DISPLAY_NAME, "1", "manual_dataset", "confirmed")
    root_key = sha(encode({'issuer': ISSUER, 'pipeline': PIPELINE, 'run_id': CONFIG['run_id'], 'files': sorted(x['sha256'] for x in manifest)})) if CONFIG else sha(b"spdb-pilot-manifest-v1")
    root_task = cn.execute("SELECT TaskId FROM DV.IssuerProcessingTask WHERE IdempotencyKey=?", root_key).fetchval()
    if not root_task:
        root_task = cn.execute("INSERT DV.IssuerProcessingTask(IssuerId,Stage,InputManifestKey,InputHash,IdempotencyKey,PipelineManifestHash,Status) OUTPUT inserted.TaskId VALUES(?,?,?,?,?,?,?)", issuer_id, "import_batch", "input/prepared/manifest.json", sha(encode(manifest)), root_key, sha(PIPELINE.encode()), "running").fetchval()
    cn.commit()
    result = []
    for item in manifest:
        if quarantined(item,ROOT):continue
        path = SOURCE / item["source_name"]
        if local:
            assert item['local_hash_verified'] is True
            digest=bytes.fromhex(item['sha256'])
        else:
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").digest()
            assert digest.hex() == item["sha256"] and path.stat().st_size == item["bytes"]
        file_id = cn.execute("SELECT FileId FROM DV.IssuerFile WHERE Sha256=?", digest).fetchval()
        if not file_id:
            storage_key=item['sha256']+'.pdf' if PRODUCTION else item['local_path'] if local else 'input/source/'+item['source_name']
            backend=archive['backend'] if PRODUCTION else 'operator_local' if local else 'r760_filesystem'
            registered_local=local and not PRODUCTION
            file_id = cn.execute("INSERT DV.IssuerFile(Sha256,StorageKey,ByteSize,PageCount,ValidationStatus,StorageBackendCode,ArchiveStatus,ArchivedAt) OUTPUT inserted.FileId VALUES(?,?,?,?,?,?,?,CASE WHEN ?=1 THEN NULL ELSE SYSUTCDATETIME() END)", digest, storage_key, item["bytes"], item["page_count"], "verified",backend,'registered_local' if registered_local else 'archived',int(registered_local)).fetchval()
        doc_id = cn.execute("SELECT DocumentId FROM DV.IssuerDocument WHERE IssuerId=? AND FileId=?", issuer_id, file_id).fetchval()
        is_rating = CONFIG.get('document_kind') == 'rating'
        is_risk = CONFIG.get('document_kind') in ('default','penalty')
        start, end, kind = (None, None, 'risk_'+CONFIG['document_kind']) if is_risk else (None, None, 'issuer_rating') if is_rating else period_from_name(item["source_name"])
        if not doc_id:
            doc_id = cn.execute("INSERT DV.IssuerDocument(IssuerId,FileId,Title,DocumentType,MainPeriodStart,MainPeriodEnd) OUTPUT inserted.DocumentId VALUES(?,?,?,?,?,?)", issuer_id, file_id, item["source_name"], kind, start, end).fetchval()
            if CONFIG.get('acquisition_version') and item.get('published_date'):
                cn.execute('UPDATE DV.IssuerDocument SET PublishedDate=? WHERE DocumentId=?',item['published_date'],doc_id)
        source_system = 'chinamoney' if CONFIG.get('acquisition_version') else 'manual_dataset'
        locator = item.get('source_relative',ISSUER + "/" + item["source_name"])
        if source_system == 'chinamoney':
            from acquisition.chinamoney import allowed_url
            locator = allowed_url(item['source_url'])
        locator_hash = sha(locator.encode("utf-8"))
        if not cn.execute("SELECT SourceId FROM DV.IssuerDocumentSource WHERE DocumentId=? AND SourceSystem=? AND LocatorHash=?", doc_id, source_system, locator_hash).fetchval():
            cn.execute("INSERT DV.IssuerDocumentSource(DocumentId,SourceSystem,SourceLocator,LocatorHash,LocatorNormalizationVersion,OriginalFileName,ImportRootTaskId) VALUES(?,?,?,?,?,?,?)", doc_id, source_system, locator, locator_hash, "1", item["source_name"], root_task)
        elif source_system == 'chinamoney':
            cn.execute("UPDATE DV.IssuerDocumentSource SET LastSeenAt=SYSUTCDATETIME() WHERE DocumentId=? AND SourceSystem=? AND LocatorHash=?",doc_id,source_system,locator_hash)
        cn.commit()
        result.append(dict(item, issuer_id=issuer_id, file_id=file_id, document_id=doc_id, root_task_id=root_task, period_start=start, period_end=end, document_type=kind, period_verified=False,server_archived=PRODUCTION or not local))
    atomic_json(ARTIFACTS / "registered.json", result)
    cn.close()
    print(json.dumps({"event": "registered", "files": len(result), "periods_from_names": len({x['period_end'] for x in result})}), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["bootstrap", "register", "register-local"])
    args = parser.parse_args()
    {"bootstrap": bootstrap, "register": register, "register-local":lambda:register(True)}[args.action]()


if __name__ == "__main__":
    main()
