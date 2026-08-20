# R760 crawler migration acceptance — 2026-08-20

## Accepted state

All four production crawlers are available on R760 as manual, one-shot
systemd/Compose tasks. No crawler timer or cron entry is enabled. The legacy
Kamatera VM remains powered off and is retained only as a cold rollback.

Accepted image:

`sha256:2b8c3fc28aa28a128a70c3d7c09a29b9c490f6e7baba3d8a16250e1180717768`

The final read-only preflight passed 11/11 checks. After acceptance there were
no crawler containers or Compose networks left running.

## Production runs

| Workflow | Accepted log | Result |
| --- | --- | --- |
| ABN products | `abn-products-20260820T020045Z.log` | Exit 0; 49 URLs; public 0, private 49, other 0; timestamp `2026-08-19 17:00:02` |
| ABN reports | `abn-reports-20260820T050913Z.log` | Exit 0; 13 reports completed; one genuinely missing product; timestamp `2026-08-19 18:44:00` |
| ABS issuance files | `fxwj-20260820T025759Z.log` | Exit 0; 12 products, 84 PDFs, 252 uploads, 84 document inserts; timestamp `2026-08-19 16:57:34` |
| ABS trustee reports | page logs beginning `stbg-page6-20260820T055917Z.log` through `stbg-page1-20260820T122011Z.log` | Catch-up accepted; timestamp `2026-08-20 08:30:00` |

The trustee-report coverage audit found 389/389 titles with success markers,
389/389 valid SQL business records, and zero unmatched titles. The final
zero-increment pages 6→1 canary ran from 13:55:15Z to 13:56:20Z (65 seconds);
all six status JSON files recorded exit code 0, all six pages reported zero
products, and no timestamp was advanced.

## Why the trustee catch-up was long

The prior trustee timestamp was `2026-08-07 10:52:47`, so this was a real
multi-page backfill, not a normal daily no-op. It reconciled 389 reports and
performed serial source downloads, uploads to primary/increment/secondary FTP
targets, and SQL writes. The observed FTP path was often about 40 KiB/s; a
20.5 MiB file could take roughly seven minutes for one target.

A six-hour systemd limit killed the first page-1 attempt even though progress
was continuing. The service limit is now 24 hours. SQL transaction logs also
filled during the catch-up and caused item-level failures; log backups were
taken and verified before completing idempotent repairs:

- `C:\SQLLogBackups\PortfolioManagement_log_20260820T120538Z.trn`
- `C:\SQLLogBackups\TaskCollection_log_20260820T120538Z.trn`

Final log usage was 1.81% for FixedIncomeSuite, 8.78% for
PortfolioManagement, and 31.96% for TaskCollection.

## Runtime hardening and rollback

The legacy business scripts remain the source of the workflows. Deployment
transforms isolate credentials, use the validated ODBC route, make FTP retries
idempotent, disable shared keep-alive by default, skip FTP directory scanning
when a page has zero new products, and bound initial FTP connects at 120
seconds.

The immediately preceding image is retained as
`dealviewer-crawler:r760-20260820-pre-connect-timeout120`. Deployment backups
are under:

- `/data/dealviewer-crawler/deploy_backups/20260820T045850Z-abn-prefix`
- `/data/dealviewer-crawler/deploy_backups/20260820T115945Z-timeout24h`
- `/data/dealviewer-crawler/deploy_backups/20260820T134200Z-empty-scan-guard`
- `/data/dealviewer-crawler/deploy_backups/20260820T140000Z-connect-timeout120`

Two empty, incorrectly named ABN FTP directories from an earlier failed
attempt remain because the FTP account cannot remove them. Correct files and
database rows are present; these empty directories do not affect processing.
