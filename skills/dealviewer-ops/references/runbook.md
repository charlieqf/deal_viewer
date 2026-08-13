# DealViewer Operations Reference

Use this reference with the repository's `dome1/RUNBOOK.md`, which remains the detailed script failure and repair history. Do not put management addresses or credentials in this versioned skill.

## Current Runtime State

- R760 is the only online crawler runtime.
- The accepted isolated Compose bundle is `/data/dealviewer-crawler/bundle` and covers only `fxwj2023_new.py` and `stbg_2025.py`.
- The accepted image is `sha256:21fe0eecb4ac21350a70aa93a040cead21fa0f5c973e9b9e9c166d1c6e7e4f7b`.
- Direct Chinabond access is the default; no proxy is configured, and the crawler must not reuse Gateway Mihomo.
- No crawler timer is enabled because no authoritative legacy schedule was found. Confirm business run times and overlap policy before adding one.
- `ABN2025_products_new.py` and `ABN2025_new.py` are not in the accepted R760 bundle and have no documented online runtime while Kamatera is off.

The final same-day read-only R760 preflight before Kamatera shutdown on 2026-08-13 passed both direct Chinabond business/sample-PDF checks, both zero-write FTP checks, both ODBC `SELECT 1` checks, and headless Chrome. Systemd reported `Result=success` and exit code `0`.

## R760 Operations

Use the operator-local R760 access notes. Normal entry points are:

```bash
systemctl start dealviewer-crawler@preflight.service
systemctl start --no-block dealviewer-crawler@fxwj.service
systemctl start --no-block dealviewer-crawler@stbg-page1.service
systemctl start --no-block dealviewer-crawler@stbg.service
systemctl show dealviewer-crawler@fxwj.service \
  -p ActiveState -p SubState -p Result -p ExecMainStatus
journalctl -u dealviewer-crawler@fxwj.service --no-pager
```

The services are one-shot jobs and remove their containers after completion. A zero exit status is not sufficient: inspect the matching status record, business counters, FTP timestamp, log errors, and cache `.error` files.

Accepted production validation:

- `stbg-page1` exited `0`, found no increment, and preserved the FTP timestamp `2026-08-07 10:52:47`.
- The resumed issuance run exited `0`, completed two products with seven associated documents each, and advanced the FTP timestamp to `2026-08-11 16:02:57`.
- The crawler left no public listener or residual container, and the R760 Gateway remained healthy.

## Kamatera Cold Rollback

Kamatera completed an operating-system poweroff at `2026-08-13T08:15:59Z`; SSH was then confirmed unreachable. The provider instance and disk remain retained and were not cancelled or deleted.

Before poweroff:

- OpenCode and its state were permanently deleted.
- Both legacy MySQL containers, their data, and onlytrade were permanently deleted.
- WeCom and Redis were disabled, and the WeCom cron entry was removed.
- job-search was stopped with restart disabled; its container and code were retained.
- The crawler service remained inactive and disabled.

Enabled base services and credentials were not removed. A cold boot can restore SSH, Nginx, and L2TP/IPsec listeners, while the remote GitLab reverse tunnel may reconnect. Audit listeners, established connections, Nginx routes, VPN sessions, Docker state, and scheduled jobs before treating the booted host as isolated or enabling a crawler.

Treat the VM as a cold rollback only. Starting the retained instance, cancelling it, or deleting its disk requires explicit authorization. For an approved crawler rollback:

1. Prove no R760 writer is running.
2. Start the retained VM through the provider console.
3. Verify the expected host identity using operator-local access notes.
4. Confirm the legacy crawler service remains disabled and no writer started automatically.
5. Revalidate direct/proxy HTTP behavior, FTP, SQL Server, browser, secrets, cache timestamps, and disk capacity.
6. Enable or run a legacy writer only after the writer fence and dependency checks pass.

The legacy workdir was `/root/deal_viewer/ABSDaily/ABS/dome1`, using `/root/deal_viewer/ABSDaily/ABS/venv/bin/python`. Do not assume those paths or dependencies remain valid after a cold boot.

## Script Boundaries

### `stbg_2025.py`

- R760 supports page 1 canary and full pages `6,5,4,3,2,1` through systemd.
- A status of `0` can hide item-level failures. Check page counters, `Matched:`, `Error occurred while processing`, timestamp behavior, and cache `.error` files.
- SQL blocking can make the process appear idle; inspect database waits before terminating it.
- Repair an individual failed FTP item instead of rerunning a completed incremental range after its timestamp has advanced.

### `fxwj2023_new.py`

- R760 runs direct-first with an optional explicit proxy fallback.
- FTP control-channel failures require reconnect and an idempotent recheck before retrying writes.
- Always validate final SQL document counts and the FTP timestamp, not only the process exit code.

### Legacy-only ABN scripts

- `ABN2025_products_new.py` and `ABN2025_new.py` have not been migrated into the accepted R760 bundle.
- Do not start them on R760 based on filename similarity or assume Kamatera is reachable.
- Migrate and validate them separately, or perform the explicit cold-rollback procedure.
- Keep FTP keep-alive disabled for `ABN2025_new.py` unless deliberately testing the repaired control-channel behavior.

## Common Failure Rules

- For SQL Server `LOG_BACKUP`, prefer a transaction-log backup while using FULL recovery. Switching to SIMPLE changes restore continuity and requires explicit approval.
- For impossible FTP paths, wrong replies, or `NoneType` read errors, suspect keep-alive/control-channel interleaving before assuming the server path is absent.
- If no `.status` file exists, first determine whether the process is still running and whether a shell wrapper exited before writing status.
- Use PowerShell here-strings piped to remote `bash -s` for complex SSH commands; strip carriage returns when arguments are sensitive.
- Never print, document, or commit passwords, tokens, private keys, raw secret files, or management endpoints.
