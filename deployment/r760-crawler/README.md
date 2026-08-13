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
- The final same-day read-only preflight before the Kamatera shutdown on
  2026-08-13 passed direct Chinabond access, both zero-write FTP checks, both
  ODBC checks, and headless Chrome with no configured proxy. The unit result
  was `success` with exit code `0`.

## Legacy Kamatera state

Kamatera completed an operating-system poweroff at `2026-08-13T08:15:59Z`,
and SSH was confirmed unreachable. The provider instance and disk remain
retained; they were not cancelled or deleted.

Before shutdown, OpenCode and its state, both legacy MySQL containers and their
data, and onlytrade were permanently deleted. WeCom and Redis were disabled,
the WeCom cron entry was removed, and job-search was stopped with restart
disabled while its container and code were retained. Treat the VM only as a
cold crawler rollback: boot it with explicit
approval, first prove the R760 writer is absent, and revalidate dependencies
before enabling any legacy writer.

Enabled base services and credentials remain on the powered-off VM. A cold boot
can restore SSH, Nginx and L2TP/IPsec listeners, and the remote GitLab reverse
tunnel may reconnect; audit listeners and consumers before using the host.

The accepted R760 bundle contains only `fxwj2023_new.py` and `stbg_2025.py`.
It does not provide an online runtime for `ABN2025_products_new.py` or
`ABN2025_new.py` while Kamatera remains powered off.
