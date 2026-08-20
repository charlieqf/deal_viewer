---
name: dealviewer-ops
description: Operate and troubleshoot the four migrated DealViewer ABSDaily crawlers on the R760 isolated manual runtime, or assess the powered-off Kamatera cold rollback. Use for ABN products/reports, ABS issuance files, ABS trustee reports, FTP/SQL validation, deployment fixes, runtime status, and rollback work.
---

# DealViewer Ops

## Operating Rule

Before production actions, read `references/runbook.md`. It contains the current host state, runtime paths, run commands, script-specific notes, and failure playbooks.

## Default Context

- R760 is the only online crawler runtime. Use operator-local access notes; do not copy management endpoints into repository documentation.
- The isolated bundle is `/data/dealviewer-crawler/bundle`; normal entry points are the `dealviewer-crawler@.service` one-shot instances.
- Accepted scope covers `ABN2025_products_new.py`, `ABN2025_new.py`, `fxwj2023_new.py`, and `stbg_2025.py`. Read `references/runbook.md` for the current image, accepted counters, timestamps, and rollback tags instead of relying on values copied into this entry point.
- All crawler jobs are manual. No crawler timer or cron entry is enabled; do not add scheduling unless the user explicitly requests it.
- Kamatera completed an OS poweroff on 2026-08-13. Its provider instance and disk remain retained, not cancelled or deleted.
- Before poweroff, OpenCode, both legacy MySQL databases, and onlytrade were permanently deleted. WeCom and Redis were disabled, the WeCom cron was removed, and job-search was stopped with restart disabled while its container and code were retained.
- A cold boot can restore enabled infrastructure listeners such as SSH, Nginx, and L2TP/IPsec, and the remote GitLab reverse tunnel may reconnect. Audit listeners and consumers before treating the booted VM as isolated.
- The legacy Kamatera workdir/Python and current R760 access details live in operator-local notes and `references/runbook.md`.
- Main local mirrors are `ABSDaily/ABS/dome1` and `deal_viewer/dome1`.

Do not expose or add passwords to docs, logs, commits, or prompts. Use existing environment/config on the host.

## Workflow

1. Identify the exact crawler, requested pages or date range, and whether the run is a normal increment, zero-increment canary, or historical backfill.
2. Read `references/runbook.md`, prove no R760 writer is already running, and run the read-only preflight before a production writer.
3. For a large backfill, inspect the prior FTP timestamp, expected item/file volume, SQL transaction-log usage, and available log-backup path before starting.
4. Use the installed one-shot systemd services and inspect `/data/dealviewer-crawler/logs`. Use Kamatera only after an explicit rollback decision, host verification, an R760 writer fence, and fresh dependency checks.
5. Monitor business progress rather than log activity alone: item matches, downloads, FTP target uploads, SQL writes, reconnects, network throughput, and database waits.
6. At completion, verify status JSON, business counters, FTP timestamp, cache `.error` and `.success` markers, and SQL business rows. Exit code `0` is necessary but not sufficient.
7. If changing code, preserve the established business logic where possible. Patch both local mirrors, add or update behavior-focused tests, back up the remote bundle/image, deploy, run preflight, and perform a zero-increment acceptance canary.

Use a here-string piped to `ssh ... 'bash -s'` for complex remote shell from Windows PowerShell. One-line commands with nested quotes are easy to break. When arguments or shell code are sensitive to carriage returns, pipe through `tr -d '\r' | bash -s` on the VM side.

## Performance Triage

- A full trustee pages 6→1 zero-increment canary has an accepted baseline of about 65 seconds. If it exceeds five minutes, inspect initial FTP connectivity, HTTP fallback, SQL waits, and whether an empty page is incorrectly scanning FTP directories.
- A historical backfill can legitimately take hours because downloads, three FTP targets, and SQL writes are serial. Slow but increasing business counters or network bytes are progress; do not terminate solely because logs are temporarily quiet.
- Initial FTP connects are bounded at 120 seconds. The systemd outer limit is 24 hours so it does not kill a progressing backfill.
- Treat SQL transaction-log capacity as a crawler dependency. Prefer a verified transaction-log backup under FULL recovery; do not switch recovery models without explicit authorization.

## Safety Notes

- Do not run duplicate copies of the same production script unless the user explicitly asks for parallel runs and the script is known to be idempotent.
- Do not treat Kamatera SSH failure as an incident while its documented power state is off. Starting the provider instance, cancelling it, or deleting its retained disk requires explicit user authorization.
- All four production crawlers are migrated. Do not run the legacy Kamatera copies while R760 is the active writer.
- Ask before destructive database maintenance unless the user has already directed it. Switching SQL Server recovery from FULL to SIMPLE breaks point-in-time restore/log-backup continuity.
- For FTP errors that look like wrong replies, missing the Chinese-named incremental-docs directory, or impossible `550` path failures, suspect keep-alive/control-channel interleaving before assuming the remote tree is missing.
- Do not manually update FTP timestamp files unless the relevant script run truly completed.
