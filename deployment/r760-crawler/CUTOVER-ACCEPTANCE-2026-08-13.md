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
- Fresh 2026-08-13 read-only preflight: both Chinabond business/sample-PDF checks direct; both FTP lists zero-write; both ODBC `SELECT 1` checks and headless Chrome passed; no proxy configured.

## Writer fence and rollback

- Kamatera `dealviewer-ops.service` is inactive and disabled; no crawler process runs there.
- Kamatera code and VM are retained for explicit rollback. Do not enable it until the R760 writer is stopped and verified absent.
- This acceptance covers crawler cutover only. Kamatera still hosts active non-crawler services (including Nginx, WeCom, MySQL, Redis, L2TP/IPsec, and three Docker containers), so it is not approved for shutdown or cancellation.
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
