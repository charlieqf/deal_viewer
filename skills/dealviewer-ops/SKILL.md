---
name: dealviewer-ops
description: Operate DealViewer ABSDaily scripts on the R760 isolated crawler runtime and assess or recover the powered-off legacy Kamatera cold rollback. Use when asked to run, rerun, monitor, deploy fixes, troubleshoot, document runtime status, assess host shutdown or rollback, or handle scripts such as ABN2025_products_new.py, ABN2025_new.py, stbg_2025.py, fxwj2023_new.py, day_fxjg2023_new.py, and related FTP or SQL Server workflows.
---

# DealViewer Ops

## Operating Rule

Before production actions, read `references/runbook.md`. It contains the current host state, runtime paths, run commands, script-specific notes, and failure playbooks.

## Default Context

- Primary runtime: R760; use operator-local access notes instead of publishing the concrete management endpoint in the repository.
- R760 bundle: `/data/dealviewer-crawler/bundle`; normal operator entry point is `dealviewer-crawler@.service`.
- Accepted R760 scope: `fxwj2023_new.py` and `stbg_2025.py`. The accepted image is `sha256:21fe0eecb4ac21350a70aa93a040cead21fa0f5c973e9b9e9c166d1c6e7e4f7b`; direct mode needs no proxy. No crawler timer is enabled.
- `ABN2025_products_new.py` and `ABN2025_new.py` are not in the accepted R760 bundle and have no documented online runtime while Kamatera is off.
- Legacy Kamatera completed an OS poweroff at `2026-08-13T08:15:59Z`; SSH was then confirmed unreachable. The provider instance and disk remain retained, not cancelled or deleted.
- Before poweroff, OpenCode, both legacy MySQL databases, and onlytrade were permanently deleted. WeCom and Redis were disabled, the WeCom cron was removed, and job-search was stopped with restart disabled while its container and code were retained.
- A cold boot can restore enabled infrastructure listeners such as SSH, Nginx, and L2TP/IPsec, and the remote GitLab reverse tunnel may reconnect. Audit listeners and consumers before treating the booted VM as isolated.
- Legacy workdir/Python after an approved boot: `/root/deal_viewer/ABSDaily/ABS/dome1` and `/root/deal_viewer/ABSDaily/ABS/venv/bin/python`.
- Main local mirrors: `ABSDaily/ABS/dome1` and `deal_viewer/dome1`.
- SQL Server used by current ABN scripts: `113.125.202.171,52482`, database `PortfolioManagement`.

Do not expose or add passwords to docs, logs, commits, or prompts. Use existing environment/config on the host.

## Workflow

1. Identify the exact script and desired run mode from the user request.
2. Check R760 for a running copy before starting a writer. Treat Kamatera as offline unless an explicitly approved rollback has first started the retained instance; never let both hosts write concurrently.
3. On R760, use the installed one-shot systemd services and inspect `/data/dealviewer-crawler/logs`. Use the legacy remote venv/PID/status wrapper only after an explicit Kamatera rollback decision, host-identity verification, an R760 writer fence, and fresh dependency checks.
4. Monitor the first part of the log and the final status. Report the log path, status code, meaningful business counters, cache `.error` files, and any per-item errors because a status value of `0` can still hide item-level failures.
5. If changing code, patch both local mirror copies when both exist, run a syntax check, back up the remote file, deploy, and verify the remote hash or syntax.

Use a here-string piped to `ssh ... 'bash -s'` for complex remote shell from Windows PowerShell. One-line commands with nested quotes are easy to break. When arguments or shell code are sensitive to carriage returns, pipe through `tr -d '\r' | bash -s` on the VM side.

## Safety Notes

- Do not run duplicate copies of the same production script unless the user explicitly asks for parallel runs and the script is known to be idempotent.
- Do not treat Kamatera SSH failure as an incident while its documented power state is off. Starting the provider instance, cancelling it, or deleting its retained disk requires explicit user authorization.
- Do not claim the two legacy ABN scripts have migrated to R760; either migrate and validate them separately or explicitly boot the cold rollback before using their legacy runtime.
- Ask before destructive database maintenance unless the user has already directed it. Switching SQL Server recovery from FULL to SIMPLE breaks point-in-time restore/log-backup continuity.
- For FTP errors that look like wrong replies, missing the Chinese-named incremental-docs directory, or impossible `550` path failures, suspect keep-alive/control-channel interleaving before assuming the remote tree is missing.
- Do not manually update FTP timestamp files unless the relevant script run truly completed.
