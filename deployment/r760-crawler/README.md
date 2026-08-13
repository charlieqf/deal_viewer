# DealViewer crawler on R760

This bundle runs the production `fxwj2023_new.py` and `stbg_2025.py` scripts as
isolated, one-shot containers. It publishes no ports and does not share the
Codex Gateway Compose project, network, volumes, or private Mihomo service.

## Layout on R760

- `/data/dealviewer-crawler/bundle`: Docker build and runtime files.
- `/data/dealviewer-crawler/private`: root-controlled raw rollback source and
  generated `secrets.json`.
- `/data/dealviewer-crawler/state`: host/container concurrency locks.
- `/data/dealviewer-crawler/state/*_file_cache`: the only writable paths
  mounted into the otherwise read-only application directory.
- `/data/dealviewer-crawler/logs`: crawler logs and status records.

The legacy issuance `FileHandler` is redirected to
`/data/dealviewer-crawler/logs/fxwj-legacy.log`; it cannot write into the
read-only application tree.

`prepare_sources.py` verifies the exact Kamatera production hashes, extracts
legacy literals into `private/secrets.json`, and emits secret-free runtime
sources. The proxy default is direct access; `DEALVIEWER_PROXY_URL` remains an
optional, explicit fallback and must never point at the R760 Gateway Mihomo.

The production SQL Server currently negotiates a legacy TLS protocol. The
container carries a dedicated OpenSSL compatibility profile for that endpoint;
it does not modify the R760 host OpenSSL policy. The old `pymssql` insert path
is rewritten to the same validated ODBC connection used by the rest of each
script. Remove this compatibility profile after the SQL Server is upgraded.

## Operator entry points

After deployment and validation:

```bash
systemctl start dealviewer-crawler@preflight.service
systemctl start --no-block dealviewer-crawler@fxwj.service
systemctl start --no-block dealviewer-crawler@stbg-page1.service
systemctl status dealviewer-crawler@fxwj.service
journalctl -u dealviewer-crawler@fxwj.service
```

Use `--no-block` for long crawls so the SSH command returns immediately; the
systemd job continues independently of the SSH session.

The full trustee-report sequence is `dealviewer-crawler@stbg.service`, which
runs pages `6,5,4,3,2,1`. No timer is enabled because the legacy environment
did not provide an authoritative crawler schedule. Confirm the business run
times and overlap policy before installing or enabling any timer.

## Accepted production canaries

- Trustee page 1 completed with no increment and preserved its
  `2026-08-07 10:52:47` FTP timestamp.
- The issuance workflow completed two products after one idempotent resume,
  with 14 associated-document rows in final SQL validation and the final FTP
  timestamp `2026-08-11 16:02:57`.
- The accepted image is
  `sha256:21fe0eecb4ac21350a70aa93a040cead21fa0f5c973e9b9e9c166d1c6e7e4f7b`.
  The immediately preceding image remains tagged
  `dealviewer-crawler:r760-20260811-pre-ftp-retry` for rollback.
- A fresh read-only preflight passed on 2026-08-13 with direct Chinabond access
  and no configured proxy. Kamatera is retained but its operator service is
  inactive and disabled, preventing accidental dual writers.

Crawler cutover does not authorize shutting down Kamatera. A 2026-08-13
read-only audit found active non-crawler workloads there, including Nginx,
WeCom services, MySQL, Redis, L2TP/IPsec, and three long-running Docker
containers. Inventory, assign an owner to, and migrate or retire those services
before powering off or cancelling the VM.
