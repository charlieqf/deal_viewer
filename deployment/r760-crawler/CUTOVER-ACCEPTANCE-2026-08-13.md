# DealViewer R760 crawler cutover acceptance

Accepted on 2026-08-13 after the production canaries started on 2026-08-11.

## Runtime boundary

- Primary host: R760.
- Compose project: `dealviewer_crawler`.
- Accepted image: `sha256:21fe0eecb4ac21350a70aa93a040cead21fa0f5c973e9b9e9c166d1c6e7e4f7b`.
- Rollback image tag: `dealviewer-crawler:r760-20260811-pre-ftp-retry`.
- No crawler port is published and no crawler container remains after a run.
- The crawler does not join the Gateway network or reuse Gateway Mihomo.

## Business validation

- Trustee page-1 canary: exit `0`, zero incremental products, no hidden error marker; FTP timestamp remained `2026-08-07 10:52:47`.
- Issuance resume: exit `0`, one already-complete product skipped, seven PDFs downloaded directly, 21 remaining FTP uploads and seven remaining associated-document inserts completed.
- Final SQL read-only validation: both products exist and each has seven associated documents.
- Final issuance FTP timestamp: `2026-08-11 16:02:57`.
- Final same-day 2026-08-13 read-only preflight before Kamatera shutdown: both Chinabond business/sample-PDF checks direct; both FTP lists zero-write; both ODBC `SELECT 1` checks and headless Chrome passed; no proxy configured; systemd result `success`, exit `0`.

## Writer fence and rollback

- Before shutdown, OpenCode and its state, both legacy MySQL containers and their data, and onlytrade were permanently deleted. WeCom and Redis were disabled, the WeCom cron entry was removed, and job-search was stopped with restart disabled while its container and code were retained.
- Kamatera completed an operating-system poweroff at `2026-08-13T08:15:59Z`; SSH was then confirmed unreachable. Its provider instance and disk remain retained and were not cancelled or deleted.
- Kamatera is a cold rollback only. Start it only with explicit approval, first prove the R760 writer is stopped, and revalidate the legacy host before enabling `dealviewer-ops.service` or any crawler writer.
- A cold boot can restore retained SSH, Nginx and L2TP/IPsec listeners, and the remote GitLab reverse tunnel may reconnect; audit network listeners and consumers before using the host.
- The accepted R760 scope is limited to `fxwj2023_new.py` and `stbg_2025.py`; `ABN2025_products_new.py` and `ABN2025_new.py` are not in this bundle and have no documented online runtime while Kamatera is off.
- R760 has no crawler timer because no authoritative legacy schedule was found. Run times and overlap policy require business confirmation before a timer is installed.

## Operator commands

```bash
systemctl start dealviewer-crawler@preflight.service
systemctl start --no-block dealviewer-crawler@fxwj.service
systemctl start --no-block dealviewer-crawler@stbg-page1.service
systemctl start --no-block dealviewer-crawler@stbg.service
systemctl show dealviewer-crawler@fxwj.service \
  -p ActiveState -p SubState -p Result -p ExecMainStatus
```
