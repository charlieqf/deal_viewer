# DealViewer Operations Reference

Use this reference with the repository's `dome1/RUNBOOK.md`, which remains the detailed script failure and repair history. Do not put management addresses or credentials in this versioned skill.

## Current Runtime State

- R760 is the only online crawler runtime.
- The isolated Compose bundle is `/data/dealviewer-crawler/bundle` and covers `ABN2025_products_new.py`, `ABN2025_new.py`, `fxwj2023_new.py`, and `stbg_2025.py`.
- The current image is `sha256:5635137dbb6415f40d535a5c61f9d74ec336d617e642cc345da6c023048231ee`; the prior accepted image remains tagged `dealviewer-crawler:r760-20260830-pre-fxwj-series-prefix`.
- Direct Chinabond access is the default; no proxy is configured, and the crawler must not reuse Gateway Mihomo.
- No crawler timer is enabled because no authoritative legacy schedule was found. Confirm business run times and overlap policy before adding one.
- All four crawlers are manual one-shot jobs; no crawler timer or cron entry is enabled.

The 2026-08-30 read-only R760 preflight passed all 11 dependency checks. Systemd reported `Result=success` and exit code `0`.

The 2026-08-30 routine run repaired no-year issuance TrustCode generation, completed 2 issuance products/15 PDFs/45 uploads, 27 ABN products, 7 ABN reports, and 4 trustee reports, with zero item-level errors. Before trustee writes, verified compressed transaction-log backups preserved FULL recovery and reduced the three dependent database logs to low usage. See `dome1/RUNBOOK.md` for counters, timestamps, rollback tags, and the partial-row repair record.

## R760 Operations

Use the operator-local R760 access notes. Normal entry points are:

```bash
systemctl start dealviewer-crawler@preflight.service
systemctl start --no-block dealviewer-crawler@abn-products.service
systemctl start --no-block dealviewer-crawler@abn-reports.service
systemctl start --no-block dealviewer-crawler@fxwj.service
systemctl start --no-block dealviewer-crawler@stbg-page1.service
systemctl start --no-block dealviewer-crawler@stbg.service
systemctl show dealviewer-crawler@fxwj.service \
  -p ActiveState -p SubState -p Result -p ExecMainStatus
journalctl -u dealviewer-crawler@fxwj.service --no-pager
```

The services are one-shot jobs and remove their containers after completion. A zero exit status is not sufficient: inspect the matching status record, business counters, FTP timestamp, log errors, and cache `.error` files.

Accepted 2026-08-20 production validation:

- ABN products: 49 URLs, exit `0`, timestamp `2026-08-19 17:00:02`.
- ABN reports: 13 completed reports and one genuinely missing product, exit `0`, timestamp `2026-08-19 18:44:00`.
- Issuance files: 12 products, 84 PDFs, 252 uploads, 84 document inserts, exit `0`, timestamp `2026-08-19 16:57:34`.
- Trustee reports: 389/389 success markers and valid SQL records, zero unmatched titles, timestamp `2026-08-20 08:30:00`.
- The final zero-increment trustee pages 6→1 canary took 65 seconds and produced six exit-code-0 status records. No crawler container, network, timer, or cron entry remained.

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
- Empty page results skip FTP product-directory scanning. Initial FTP connects are capped at 120 seconds, while the systemd outer limit is 24 hours for a genuine backfill.
- A status of `0` can hide item-level failures. Check page counters, `Matched:`, `Error occurred while processing`, timestamp behavior, and cache `.error` files.
- SQL blocking can make the process appear idle; inspect database waits before terminating it.
- Repair an individual failed FTP item instead of rerunning a completed incremental range after its timestamp has advanced.

### `fxwj2023_new.py`

- R760 runs direct-first with an optional explicit proxy fallback.
- FTP control-channel failures require reconnect and an idempotent recheck before retrying writes.
- Always validate final SQL document counts and the FTP timestamp, not only the process exit code.

### ABN scripts

- `ABN2025_products_new.py` and `ABN2025_new.py` are migrated into the R760 bundle as the `abn-products` and `abn-reports` one-shot tasks.
- `ABN2025_new.py` keeps FTP keep-alive disabled. Its reconnect path uses the same production FTP credentials and retries uploads idempotently.
- Validate the product/report counters, final FTP timestamp, hidden error markers, and status JSON before accepting a run.
- Use the Kamatera copies only during an explicitly approved cold rollback after proving that no R760 writer is running.

## Common Failure Rules

- For SQL Server `LOG_BACKUP`, prefer a transaction-log backup while using FULL recovery. Switching to SIMPLE changes restore continuity and requires explicit approval.
- For impossible FTP paths, wrong replies, or `NoneType` read errors, suspect keep-alive/control-channel interleaving before assuming the server path is absent.
- If no `.status` file exists, first determine whether the process is still running and whether a shell wrapper exited before writing status.
- Use PowerShell here-strings piped to remote `bash -s` for complex SSH commands; strip carriage returns when arguments are sensitive.
- Never print, document, or commit passwords, tokens, private keys, raw secret files, or management endpoints.
