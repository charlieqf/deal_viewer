# DealViewer crawler on R760

This bundle runs the production ABN product (`ABN2025_products_new.py`), ABN
report (`ABN2025_new.py`), ABS issuance-file (`fxwj2023_new.py`), and ABS
trustee-report (`stbg_2025.py`) scripts as isolated, one-shot containers. It
publishes no ports and does not share the Codex Gateway Compose project,
network, volumes, or private Mihomo service.

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
systemctl start --no-block dealviewer-crawler@abn-products.service
systemctl start --no-block dealviewer-crawler@abn-reports.service
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

## Production acceptance

The current production image is
`sha256:5635137dbb6415f40d535a5c61f9d74ec336d617e642cc345da6c023048231ee`.
On 2026-08-30 it completed the routine sequence with 2 issuance products/15
PDFs/45 uploads, 27 ABN products, 7 ABN reports, and 4 trustee reports. The
issuance fix shortens no-year series identifiers before the downstream
`nvarchar(50)` boundary; its post-run zero-increment canary exited `0` with no
writes. The prior image is retained as
`dealviewer-crawler:r760-20260830-pre-fxwj-series-prefix`.

- The 2026-08-20 ABN product run processed 49 URLs (public 0, private 49,
  other 0), exited `0`, and advanced its timestamp to
  `2026-08-19 17:00:02`.
- The 2026-08-20 ABN report run completed 13 report items, left one genuinely
  unmatched product, exited `0`, and advanced its timestamp to
  `2026-08-19 18:44:00`.
- The 2026-08-20 issuance-file run completed 12 products, 84 direct PDF
  downloads, 252 FTP uploads, and 84 associated-document inserts. It exited
  `0` and advanced its timestamp to `2026-08-19 16:57:34`.
- The 2026-08-20 trustee-report catch-up covered pages 4 through 1. Final
  validation found 389/389 titles represented by success markers and 389/389
  valid SQL business records, with no unmatched title. Its timestamp is
  `2026-08-20 08:30:00`.
- A post-run zero-increment trustee sequence completed pages 6 through 1 in
  65 seconds with six exit-code-0 status records. Empty result sets now skip
  the legacy FTP product-directory scan, and initial FTP connects are bounded
  at 120 seconds. The systemd outer limit is 24 hours for genuine backfills.
- The 2026-08-20 accepted image was
  `sha256:2b8c3fc28aa28a128a70c3d7c09a29b9c490f6e7baba3d8a16250e1180717768`.
  Earlier accepted and pre-resilience images remain tagged on R760 for
  rollback.
- The final 2026-08-20 read-only preflight passed all 11 checks: Chinabond
  issuance/trustee data and sample PDFs, ChinaMoney ABN, both zero-write FTP
  checks, four ODBC checks, secret schema, and headless Chrome.

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

All four production crawlers now have manual, one-shot R760 entry points.
There is no crawler timer or cron entry; Kamatera remains a powered-off cold
rollback and must not be started as a writer while an R760 job is running.
