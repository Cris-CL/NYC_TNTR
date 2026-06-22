# Tantra Data Pipeline

This repo is a set of Google Cloud Functions that together move data into BigQuery from two sources:

1. **`.xlsx` data files** uploaded by a person into a watched Google Drive (Shared Drive) folder — the main event-driven pipeline.
2. **Google Sheets master/reference data** (wages, prices, extra charges, online orders) — a sibling, request-driven pipeline.

Each function lives in its own folder and has a detailed `README.md`. This document describes the overall flow and how the functions connect.

---

## 1. Drive → Bucket → BigQuery (main pipeline)

A human drops `.xlsx` files into a shared Drive folder. The pipeline detects them, stages them in a GCS bucket, announces them, and a downstream consumer validates and loads them into BigQuery.

```
┌────────────────────────┐
│ wh_listener_scheduler  │  Cloud Scheduler → Pub/Sub, runs periodically
│  starter()             │  Registers/renews the Drive files().watch on the folder
└───────────┬────────────┘  (Drive watch channels expire, so this must re-run)
            │ creates/refreshes the watch channel
            ▼
      ╔═══════════════╗
      ║ Google Drive  ║  Watched shared folder. On any change, Drive POSTs a
      ║ (shared drive)║  notification to the registered webhook URL.
      ╚═══════╤═══════╝
              │ change notification (HTTP POST)
              ▼
┌────────────────────────┐
│ webhook_tantra         │  Public webhook listener (Flask /webhook)
│  webhook()             │  • ignores `sync` / `trash` notifications
└───────────┬────────────┘  • fire-and-forget POST (1s timeout) to DRIVE_FUNCTION_URL
            │ triggers (does not wait)
            ▼
┌────────────────────────┐
│ tantra_drive_bucket    │  full_process()
│  full_process()        │  • read last token from BigQuery tracker
│                        │  • fetch Drive changes since token
│                        │  • move new .xlsx files → GCS stage bucket
└───────────┬────────────┘  • log changes + new token back to BigQuery
            │ object created in the stage bucket
            ▼
      ╔═══════════════╗
      ║  GCS bucket   ║  Stage bucket. New object = "object finalize" event.
      ╚═══════╤═══════╝
              │ object-finalize event
              ▼
┌────────────────────────┐
│ tantra_msg_send        │  send_message(event, context)
│  send_message()        │  Publishes {file_name, file_uri} to a Pub/Sub topic
└───────────┬────────────┘
            │ Pub/Sub message
            ▼
┌────────────────────────┐
│ downstream consumer    │  (subscriber to the topic — not in these folders)
│  (not in this repo set)│  Reads gs:// file, validates contents/naming fit the
│                        │  target table, loads a NEW VERSION of the data to BigQuery
└────────────────────────┘
```

### Connection summary (who triggers whom)

| Function | Trigger | Does | Hands off to |
|---|---|---|---|
| `wh_listener_scheduler` | Scheduler/Pub/Sub (timed) | Registers/renews the Drive `files().watch` | Drive (so it will POST to `webhook_tantra`) |
| `webhook_tantra` | HTTP POST from Google Drive | Filters noise, fire-and-forget trigger | `tantra_drive_bucket` (via `DRIVE_FUNCTION_URL`) |
| `tantra_drive_bucket` | HTTP POST from `webhook_tantra` | Moves `.xlsx` Drive → GCS, logs token to BQ | GCS stage bucket |
| `tantra_msg_send` | GCS object-finalize event | Publishes file info to Pub/Sub | Pub/Sub topic → downstream consumer |
| downstream consumer | Pub/Sub subscription | Validates + loads to BigQuery | BigQuery |

### Key characteristics & known weak point
- The pipeline is a **chain of independent event-driven functions**, each with a single responsibility.
- **The fragile hop is `Drive → webhook_tantra → tantra_drive_bucket`.** The Google Drive Changes/push API is *best-effort*: it can silently drop change events, so a freshly uploaded file may never trigger processing (observed in practice). The `tantra_drive_bucket` discovery step relies on this feed plus a Drive change-token stored in BigQuery.
- The **`tantra_msg_send` hop is reliable** — it runs on GCS object-finalize events, which fire on every upload (including overwrites).
- Recommended hardening (not yet implemented): make `tantra_drive_bucket` discover files by **reconciling the folder with `files().list`** (keyed by `fileId` + `md5Checksum`) instead of trusting the change feed, so dropped events don't cause silent data loss. See [tantra_drive_bucket/README.md](tantra_drive_bucket/README.md).

---

## 2. Google Sheets → BigQuery (sibling master-data pipeline)

This pipeline is **independent** of the Drive flow above. It refreshes master/reference tables in BigQuery from curated Google Sheets, on demand.

```
   caller (manual / scheduled HTTP POST with {"type", "sheetName"})
            │
            ▼
┌────────────────────────┐
│ tantra_master_updater  │  update_master()  — Flask /update_master router
│  update_master()       │  dispatches by `type`:
│   ├─ wages   → wages.update_wages(sheetName)
│   ├─ prices  → prices.update_prices(sheetName)
│   ├─ extra   → extra.update_extra("Extra")
│   └─ online  → online_orders.update_online_sales("Transfer")
└───────────┬────────────┘
            │ each updater reads a Google Sheet, transforms, writes to BigQuery
            ▼
      ╔═══════════════╗
      ║   BigQuery    ║  wages / prices: append-then-prune by period (latest UPLOADED wins)
      ║               ║  extra / online: full table replace
      ╚═══════════════╝
```

See [tantra_master_updater/README.md](tantra_master_updater/README.md) for the per-table schemas and transform logic.

---

## Folder → README index

| Folder | Role | README |
|---|---|---|
| [wh_listener_scheduler](wh_listener_scheduler/) | Renews the Drive watch channel | [README](wh_listener_scheduler/README.md) |
| [webhook_tantra](webhook_tantra/) | Public Drive-change webhook listener | [README](webhook_tantra/README.md) |
| [tantra_drive_bucket](tantra_drive_bucket/) | Moves `.xlsx` from Drive to the stage bucket | [README](tantra_drive_bucket/README.md) |
| [tantra_msg_send](tantra_msg_send/) | Bucket object-finalize → Pub/Sub publisher | [README](tantra_msg_send/README.md) |
| [tantra_master_updater](tantra_master_updater/) | Sheets → BigQuery master-data updater | [README](tantra_master_updater/README.md) |

---

## Authentication (all functions)
Every function uses **Application Default Credentials** (the runtime service account). Required access:
- **Drive** (read changes, register watches, download files) — `wh_listener_scheduler`, `webhook_tantra` (only forwards), `tantra_drive_bucket`.
- **GCS** (read/write the stage bucket) — `tantra_drive_bucket`, `tantra_msg_send`.
- **Pub/Sub** (publish) — `tantra_msg_send`.
- **BigQuery** (read token / write data) — `tantra_drive_bucket`, downstream consumer, `tantra_master_updater`.
- **Sheets** (via gspread) — `tantra_master_updater`.

Configuration is supplied via environment variables per function (documented in each README); `.env` files are not committed/documented.
