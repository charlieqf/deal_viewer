# DealViewer Script Operations Runbook

This runbook captures current operational practice for DealViewer ABSDaily scripts on the R760 and the legacy Kamatera VM. Update it whenever a production fix changes how scripts are run.

## R760 Crawler Runtime (2026-08-11)

The ABS issuance-file (`fxwj2023_new.py`) and trustee-report (`stbg_2025.py`) jobs run in an isolated Docker Compose project on the R760. Docker is not a functional requirement for the Python code, but it is the production isolation boundary for the pinned Chrome/Driver, ODBC libraries, and the legacy TLS policy required by the old SQL Server. Do not move that TLS policy onto the R760 host.

- SSH: use the operator-local DealViewer access notes; do not publish the concrete management endpoint in the repository.
- Bundle: `/data/dealviewer-crawler/bundle`.
- Private source and secrets: `/data/dealviewer-crawler/private` (root/operator controlled; never print or copy secrets into logs).
- Persistent cache: `/data/dealviewer-crawler/state`.
- Logs and status JSON: `/data/dealviewer-crawler/logs`.
- Compose project/network: `dealviewer_crawler`; it exposes no public port and is not attached to the Gateway network.
- Runtime user is non-root; the root filesystem is read-only; CPU, memory, PID, capability, and log-size limits are set in Compose.
- HTTP mode is direct-first and currently passes Chinabond business-response and sample-PDF checks without a proxy. `DEALVIEWER_PROXY_URL` is empty by default. Do not reuse the R760 Gateway's Mihomo service for this crawler.

Operators use systemd as the only normal entry point; direct Compose knowledge is not required:

```bash
# Full read-only dependency check
systemctl start dealviewer-crawler@preflight.service
journalctl -u dealviewer-crawler@preflight.service -n 100 --no-pager

# ABS issuance files
systemctl start --no-block dealviewer-crawler@fxwj.service

# Trustee report page-1 canary or full page sequence 6..1
systemctl start --no-block dealviewer-crawler@stbg-page1.service
systemctl start --no-block dealviewer-crawler@stbg.service

# Inspect a run
systemctl show dealviewer-crawler@fxwj.service \
  -p ActiveState -p SubState -p Result -p ExecMainStatus
ls -lt /data/dealviewer-crawler/logs | head
```

`--no-block` returns control to the SSH session immediately; systemd keeps the
job running if the SSH connection closes.

These are one-shot services: the container is removed after completion. A zero process exit is necessary but not sufficient; inspect the matching status JSON, business progress/error markers, FTP update timestamp, and any cache error artifacts before declaring success.

No timer is enabled because the legacy host had no authoritative crawler schedule. Add timers only after the business run times and overlap policy are explicitly confirmed. Keep the Kamatera code and timestamp backup as the rollback boundary until the R760 production canaries have been accepted; never let both hosts run a writer concurrently.

### 2026-08-11 R760 cutover acceptance

- `stbg-page1` completed with exit code `0`, zero incremental products and no hidden errors. Its FTP timestamp correctly remained `2026-08-07 10:52:47`.
- The first `fxwj` attempt exposed a stale FTP control-channel failure while creating the second product's 211 subdirectory. It exited nonzero and did not advance the FTP timestamp. The R760 source transform now rebuilds the FTP control connection after every protocol/network list or upload error and rechecks idempotently before writing.
- The `fxwj` resume completed with exit code `0`, skipped the already completed first product, directly downloaded seven PDFs, completed 21 remaining FTP target uploads and inserted seven remaining document records. Final SQL validation found both products and seven associated documents per product; the two-product total is 14 documents. The final FTP timestamp is `2026-08-11 16:02:57`.
- A fresh full read-only preflight on `2026-08-13` passed both Chinabond business/sample-PDF checks in direct mode, both FTP list checks with zero writes, both ODBC `SELECT 1` checks and headless Chrome. No proxy was configured.
- The crawler left no container or public listener behind, and the existing R760 Gateway remained healthy. The accepted crawler image is `sha256:21fe0eecb4ac21350a70aa93a040cead21fa0f5c973e9b9e9c166d1c6e7e4f7b`; the pre-FTP-retry image is retained under rollback tag `dealviewer-crawler:r760-20260811-pre-ftp-retry`.
- Kamatera `dealviewer-ops.service` is inactive and disabled as of `2026-08-13`; no crawler process runs there. Its code and VM remain intact for an explicit rollback. Before re-enabling it, prove the R760 crawler is stopped so the two hosts cannot write concurrently.
- Do not power off or cancel the Kamatera VM based only on this crawler cutover. A 2026-08-13 read-only inventory found unrelated active Nginx, OpenCode, WeCom callback/worker, Redis, L2TP/IPsec, a job-search container and two MySQL containers, with multiple public listeners. Identify an owner and migrate or explicitly retire every remaining workload before host shutdown.

## Environment

- VM: `root@104.238.213.119`.
- Workdir: `/root/deal_viewer/ABSDaily/ABS/dome1`.
- Python: `/root/deal_viewer/ABSDaily/ABS/venv/bin/python`.
- Logs: `/root/deal_viewer/ABSDaily/ABS/dome1/logs`.
- Local mirrors: `ABSDaily/ABS/dome1` and `deal_viewer/dome1`.
- Current ABN DB endpoint in active scripts: `113.125.202.171,52482`, database `PortfolioManagement`.
- Ignore old `172.16.6.143\mssql` entries when they only appear in comments.

Never document passwords. Use the VM's existing config, environment, and SSH key setup.

## Legacy Kamatera Background Run

Use this only for an explicitly approved rollback to Kamatera. This wrapper records a PID, log, and final status:

```bash
cd /root/deal_viewer/ABSDaily/ABS/dome1
mkdir -p logs
script=ABN2025_new.py
ts=$(date -u +%Y%m%dT%H%M%SZ)
log=logs/${script%.py}_${ts}.log
status=logs/${script%.py}_${ts}.status
pidfile=logs/${script%.py}_${ts}.pid
(
  PYTHONUNBUFFERED=1 /root/deal_viewer/ABSDaily/ABS/venv/bin/python -u "$script"
  rc=$?
  echo "$rc" > "$status"
  exit "$rc"
) > "$log" 2>&1 &
echo $! > "$pidfile"
printf 'pid=%s\nlog=%s\nstatus=%s\n' "$(cat "$pidfile")" "$log" "$status"
```

Avoid `set -e` inside this wrapper before the status write, because a non-zero Python exit can skip the `.status` file.

Check a run:

```bash
ps -ef | grep ABN2025_new.py | grep -v grep
tail -n 80 logs/ABN2025_new_YYYYMMDDTHHMMSSZ.log
cat logs/ABN2025_new_YYYYMMDDTHHMMSSZ.status
```

From Windows PowerShell, use a here-string for complex remote shell:

```powershell
@'
cd /root/deal_viewer/ABSDaily/ABS/dome1
pwd
'@ | ssh root@104.238.213.119 'bash -s'
```

If the script body or arguments are sensitive to Windows CRLF endings, strip carriage returns on the VM side:

```powershell
@'
cd /root/deal_viewer/ABSDaily/ABS/dome1
pwd
'@ | ssh -i $env:USERPROFILE\.ssh\kamatera root@104.238.213.119 "tr -d '\r' | bash -s"
```

## stbg_2025.py

Purpose: trusted report data crawl.

Current script supports:

- `STBG_PAGE_NUM`: page number to crawl. Default is `6`.
- `STBG_WRITE_UPDATE_LOG=auto`: only page `1` writes the final timestamp.
- `STBG_BROWSER_WARMUP=0`: skip browser warmup.

Use the page-range wrapper when asked to run from page 6 down to page 1:

```bash
cd /root/deal_viewer/ABSDaily/ABS/dome1
./run_stbg_2025_pages.sh --background
```

The wrapper defaults to `6 5 4 3 2 1`, prevents another `stbg_2025.py` copy from running, and writes log/status/pid files under `logs/`. A status value of `0` means the sequence completed.

Known successful example:

- Log: `logs/stbg_2025_pages_6_to_1_20260619T024239Z.log`.
- Status: `logs/stbg_2025_pages_6_to_1_20260619T024239Z.status` contained `0`.

2026-07-01 run notes:

- Log: `logs/stbg_2025_pages_6_5_4_3_2_1_20260701T015401Z.log`.
- Status: `logs/stbg_2025_pages_6_5_4_3_2_1_20260701T015401Z.status` contained `0`.
- Repair log: `logs/stbg_2025_repair_20260701T022718Z.log`, status `0`.
- Pages `6 5 4 3 2 1` all exited `0`. Pages 6 through 2 had `Processing products... (total: 0)`. Page 1 had `Processing products... (total: 10)`.
- Of the 10 new page-1 items, 8 matched product folders and were finally inserted. Six completed in the main run and two were repaired manually: `AnHui_SmallLoan2026-1` and `ChangXingYe_SmallLoan2022-1`.
- Two new page-1 items were not matched by the current product-folder logic: DingYou 2025 third consumer-loan report and HuiYuan 2025 seventh NPL report. Treat these as matching/product-code follow-up, not script crashes.
- The page-1 timestamp was written to `2026-06-29 16:57:16`; do not rely on rerunning the normal incremental path to pick up item-level failures after that timestamp is written.

Post-run checks for `stbg_2025.py`:

```bash
cd /root/deal_viewer/ABSDaily/ABS/dome1
log=logs/stbg_2025_pages_6_5_4_3_2_1_YYYYMMDDTHHMMSSZ.log
status=logs/stbg_2025_pages_6_5_4_3_2_1_YYYYMMDDTHHMMSSZ.status
cat "$status"
grep -E 'Processing products|===== .*pageNum=|Completed at|FINAL_EXIT=' "$log"
grep -c 'Matched:' "$log"
grep -c 'Error occurred while processing' "$log"
find stbg_file_cache -maxdepth 1 -name '*.error' -printf '%TY-%Tm-%Td %TH:%TM:%TS %p\n' | sort | tail -n 20
```

For this script, status `0` only means the page sequence exited cleanly. Always check `Processing products`, `Matched:`, `Error occurred while processing`, and recent `stbg_file_cache/*.error` files before reporting business success.

Known `stbg_2025.py` issues:

- Windows PowerShell here-strings can pass a trailing carriage return into page arguments. If explicit page arguments produce `Invalid page number`, use the wrapper defaults or pipe through `tr -d '\r' | bash -s`.
- SQL Server blocking can make page 1 look stuck while it is waiting on the database. In the 2026-07-01 run, the script waited on `TaskCollection.dbo.ProductsStateInformation` with `LCK_M_S` and `blocking_session_id=131`, caused by an open SSMS transaction from another host. Use SQL Server DMVs to confirm `wait_type`, `blocking_session_id`, and SQL text; do not kill external sessions unless rollback is explicitly acceptable.
- FTP2 keep-alive can interfere with an active upload on the same FTP object. In the 2026-07-01 run it caused one item to fail with `'NoneType' object has no attribute 'readline'` after a keep-alive reconnect. Repair the specific item instead of rerunning the whole incremental job after the timestamp has advanced.
- `AnHui2026-1_SmallLoan2025AnHui2026-1` was not a valid TrustCode for the 2026-07-01 item. The correct code was `AnHui_SmallLoan2026-1`.
- Avoid importing `stbg_2025.py` only to repair one item; module import initializes global FTP/DB/browser side effects. Prefer a small standalone repair script that reads existing VM config, uses cached PDFs, uploads to `TrustAssociatedDoc/<TrustCode>/TrusteeReport/`, and inserts the missing DV, disclosure, and task-state rows. Do not print secrets.
- When repairing Chinese-named files from Windows, do not hard-code Chinese strings through nested PowerShell/bash quoting. Read the actual UTF-8 filenames from the VM cache, or locate them by ASCII fragments such as year/trust code.

## ABN2025_products_new.py

Purpose: crawl ABN product rows and update product metadata.

Run:

```bash
cd /root/deal_viewer/ABSDaily/ABS/dome1
PYTHONUNBUFFERED=1 /root/deal_viewer/ABSDaily/ABS/venv/bin/python -u ABN2025_products_new.py
```

Use the standard background wrapper for long runs.

Recent production failure and fix:

- Failure log: `logs/ABN2025_products_new_20260622T004636Z.log`.
- Error: SQL Server transaction log for `PortfolioManagement` was full due to `LOG_BACKUP`.
- User changed recovery model to SIMPLE and shrank the log.
- Rerun succeeded in `logs/ABN2025_products_new_20260622T012628Z.log` with status `0`.
- Successful run processed 48 URLs: public 2, private 46, other 0, `err1=0`, `LOG_BACKUP=0`.
- The product timestamp file was updated to `2026-06-18 02:10:05`.

Maintenance note: if SQL Server reports `LOG_BACKUP`, the durable fix is a log backup while in FULL recovery. If the user accepts SIMPLE recovery, switch to SIMPLE, checkpoint, and shrink the log; document that this interrupts point-in-time restore/log-backup continuity.

## ABN2025_new.py

Purpose: crawl ABN trustee/asset operation report data and upload report documents.

Default run should disable FTP keep-alive:

```bash
cd /root/deal_viewer/ABSDaily/ABS/dome1
ABN_FTP_KEEPALIVE=0 PYTHONUNBUFFERED=1 /root/deal_viewer/ABSDaily/ABS/venv/bin/python -u ABN2025_new.py
```

Current patch:

- `ABN_FTP_KEEPALIVE` defaults to `0`.
- Set `ABN_FTP_KEEPALIVE=1` only when intentionally testing keep-alive behavior.
- When disabled, the script prints `FTP keep-alive threads disabled; set ABN_FTP_KEEPALIVE=1 to enable.`

Recent production failure and fix:

- First run log: `logs/ABN2025_new_20260622T015232Z.log`.
- It inserted/processed six reports, then failed during 211 FTP upload for `BangXin1ABN2025-5`.
- Error looked like the Chinese-named incremental-docs FTP directory did not exist, followed by `ftplib.error_perm: 550 The system cannot find the path specified.`
- Root cause was likely FTP keep-alive NOOP interleaving on the same control connection.
- Remote backup before patch: `deploy_backups/20260622T020029Z/ABN2025_new.py`.
- Patched remote hash: `be7ac387834fb0e47c68bf9d42ccab3c4bae9e205683b5776267dd6feee53355`.
- Manual repair completed: imported `ABN2025_new` with keep-alive disabled and ran `upload_211('BangXin1ABN2025-5')`; output included `UPLOAD_211_DONE BangXin1ABN2025-5`.
- Rerun log: `logs/ABN2025_new_resume_20260622T020142Z.log`.
- Rerun status: `0`, error count 0.
- The ABN report timestamp file was updated to `2026-06-20 00:51:00`.

Manual 211 repair pattern, only when DB work completed but 211 upload failed for a known trust code:

```bash
cd /root/deal_viewer/ABSDaily/ABS/dome1
ABN_FTP_KEEPALIVE=0 /root/deal_viewer/ABSDaily/ABS/venv/bin/python - <<'PY'
import ABN2025_new as m
m.upload_211('TRUST_CODE_HERE')
print('UPLOAD_211_DONE TRUST_CODE_HERE')
PY
```

## fxwj2023_new.py

Purpose: related report/document workflow using trust codes and FTP uploads.

Run through the same venv/background pattern:

```bash
cd /root/deal_viewer/ABSDaily/ABS/dome1
PYTHONUNBUFFERED=1 /root/deal_viewer/ABSDaily/ABS/venv/bin/python -u fxwj2023_new.py
```

Maintenance notes:

- 2026 trust code generation was fixed to use `trust_code_utils.build_trust_code()` instead of hard-coded `2025`.
- Nine malformed trust codes were cleaned from production DB/FTP during the prior repair.
- `ftp_session_utils.py` provides explicit reconnect credentials and keep-alive locking; use it for future FTP-heavy fixes instead of sharing an unlocked `ftplib.FTP` object across threads.
- Always use the remote venv, not `/usr/bin/python3`.

## Common Failures

SQL Server log full due `LOG_BACKUP`:

- Confirm the exact error in the log.
- Ask whether the user accepts SIMPLE recovery if not already stated.
- Preferred FULL recovery fix is a log backup.
- SIMPLE workaround is recovery model SIMPLE, checkpoint, shrink log, then rerun.
- Record the choice because it changes restore/log-backup behavior.

FTP impossible path or wrong reply:

- Symptoms include a missing Chinese-named incremental-docs directory, unexpected `200 Type set to A/I`, or `550 The system cannot find the path specified` for a path that should exist.
- Suspect keep-alive/control-channel interleaving when a background thread uses the same FTP object. `stbg_2025.py` can also hit this as `NoneType`/`readline` errors during upload after FTP2 keep-alive reconnects.
- Disable keep-alive or protect FTP commands with a shared lock/reconnect helper.
- If the script committed DB work before the FTP failure, repair the missing upload and then rerun/resume.

SQL blocking or apparent hangs:

- If a Python process is alive with low CPU and no new log lines, check sockets and the current syscall before assuming it is dead.
- For SQL waits, query DMVs for `wait_type`, `blocking_session_id`, current SQL text, host, program name, and open transactions. `LCK_M_S` against `TaskCollection.dbo.ProductsStateInformation` indicates the crawler is blocked by another transaction.
- Wait for external blockers to clear when possible. Killing another user's SSMS session can roll back their transaction and should be an explicit operational decision.

No `.status` file:

- Check whether the process is still running.
- If not running, read the log tail and inspect the wrapper.
- A wrapper using `set -e` can exit before writing status.

PowerShell SSH quoting:

- Avoid dense one-liners with nested quotes, regex, or pipes.
- Prefer a PowerShell here-string piped into `ssh root@104.238.213.119 'bash -s'`.

## Deployment Notes

When patching scripts:

1. Patch both local mirrors if both copies exist.
2. Run syntax checks, for example `python -m py_compile ABN2025_new.py`.
3. Back up the remote file under `deploy_backups/<UTC timestamp>/`.
4. Copy the patched file to `/root/deal_viewer/ABSDaily/ABS/dome1`.
5. Verify remote syntax or hash before the production run.
