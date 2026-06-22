# tantra_drive_bucket

This folder contains a webhook-driven pipeline that reacts to changes in a Google Drive (Shared Drive) folder. When a file is added or modified in the watched Drive, Google sends a notification to the webhook, which then detects the new/changed `.xlsx` files and copies them into a Google Cloud Storage (GCS) bucket. Each processed change is also logged into a BigQuery table, which doubles as the store for the Drive change-tracking token.

## Where this fits across the Tantra functions

This is the middle stage of a multi-function pipeline:

```
wh_listener_scheduler  ── periodically (re)registers the Drive files().watch on the folder
        ▼
webhook_tantra         ── public listener Drive POSTs to on each change; filters noise,
        │                  fire-and-forget triggers this function
        ▼
tantra_drive_bucket    ── (THIS) pulls Drive changes, moves .xlsx files to the stage bucket,
        │                  logs the change + token to BigQuery
        ▼
tantra_msg_send        ── triggered by the new bucket object, publishes a Pub/Sub message
        ▼
(downstream consumer)  ── reads the file, validates contents/naming fit the target table,
                          loads a new version of the data into BigQuery
```

`tantra_master_updater` is a sibling pipeline (Google Sheets → BigQuery for wages/prices/extra/online master data), not part of this trigger chain. See the README in each folder for details.

## High-level flow

```
Google Drive change
        │
        ▼
  POST /webhook            (main.py)
        │
        ▼
  full_process()           (full_process.py)
        │
        ├─ get_token()                  (get_last_token.py)  ── read last token from BigQuery
        │
        ├─ fetch_changes_specific()     (get_changes.py)     ── ask Drive API for changes since token
        │
        ├─ changes_to_df()              (get_changes.py)     ── normalize changes into a DataFrame
        │
        ├─ transfer_file_between_       (move_file.py)       ── download each .xlsx from Drive,
        │     drive_and_gcs()                                   upload to the GCS bucket
        │
        └─ changes_to_bq()              (get_changes.py)     ── append processed changes + new token to BigQuery
```

## Files

### `main.py` — Webhook entry point
A Flask app exposing a `/webhook` route (intended to run as a Cloud Function / Cloud Run service).

- `GET /webhook` returns a simple HTML message (health/verification check).
- `POST /webhook` is the notification Google Drive sends when a watched change occurs. It logs the request time (Tokyo time), calls `full_process()`, and returns a JSON `{"status": "success"}` with HTTP 200.
- `get_timestamp(timezone_name)` is a helper that formats the current time in a given timezone.

The commented block at the top lists the pinned dependency versions for the deployment environment.

It depends on: `full_process.py`.

### `full_process.py` — Orchestrator
Defines `full_process()`, which wires the whole pipeline together. It reads its configuration from environment variables:

| Env var | Purpose |
|---|---|
| `PROJECT_ID` | GCP project for BigQuery |
| `DATASET` | BigQuery dataset holding the tracker table |
| `TABLE_NAME` | BigQuery tracker table (token + change log) |
| `FOLDER_ID` | Drive folder id (legacy/unused in current path) |
| `BUCKET_NAME` | Destination GCS bucket |
| `DRIVE_ID` | Shared Drive id to scope the changes query |
| `FOLDER_ID_2` | Secondary folder id (shared-folder var) |

Steps performed by `full_process()`:
1. Reads the last saved page token from BigQuery via `get_token()`.
2. Fetches Drive changes since that token via `fetch_changes_specific()`.
3. Converts the changes into a DataFrame via `changes_to_df()`; exits early if there is no new data.
4. Sorts changes ascending by modification `time`.
5. Iterates the changes and, **for files modified today** whose name ends in `.xlsx`, copies them to the bucket via `transfer_file_between_drive_and_gcs()`.
6. Appends the processed changes (including the new token) to BigQuery via `changes_to_bq()`.

It depends on: `get_last_token.py`, `get_changes.py`, `move_file.py`.

### `get_last_token.py` — Token reader
Defines `get_token(project_id, dataset, table_name)`, which queries BigQuery for `MAX(token)` from the tracker table and returns it. This token is Google Drive's start page token, marking the last point the pipeline processed so the next run only fetches newer changes.

It depends on: `google-cloud-bigquery`.

### `get_changes.py` — Drive change retrieval & BigQuery I/O
Handles talking to the Drive Changes API and shaping/storing the results.

- `fetch_changes_specific(saved_start_page_token, drive_id, creds=None)` — the function used by the current pipeline. Pages through `service.changes().list(...)` scoped to a specific Shared Drive (`includeItemsFromAllDrives=True`, `supportsAllDrives=True`), and returns the new start page token plus the raw API response.
- `fetch_changes(saved_start_page_token, creds=None)` — an older variant that queries changes without the Shared Drive parameters. Kept for reference; `full_process()` does not use it.
- `changes_to_df(token, response_json)` — normalizes the Drive response into a DataFrame, drops removed files, keeps `fileId`, `time`, `file.name`, attaches the `token`, normalizes column names, converts `time` to a tz-naive datetime, and filters down to `.xlsx` files (falling back to the unfiltered frame on error).
- `changes_to_bq(df, dataset, tracker_table)` — appends the DataFrame to the BigQuery tracker table (`if_exists="append"`).
- `df_to_dict(df)` — helper that maps `file_name → fileid` (utility; not on the main path).

It depends on: `google-api-python-client`, `pandas`, `pandas-gbq`, `pytz`.

### `move_file.py` — Drive → GCS transfer
Handles the actual file movement.

- `check_bucket(bucket_name, file_name)` — returns `True` if a blob named `file_name` already exists in the bucket, used to avoid re-uploading.
- `transfer_file_between_drive_and_gcs(drive_file_id, gcs_bucket_name, gcs_object_name, creds=None)` — downloads the file from Drive into memory (`MediaIoBaseDownload`), skips the upload if the object already exists in the bucket, and otherwise uploads it to GCS. Returns `True` on success, `False` if skipped or on error.

It depends on: `google-api-python-client`, `google-cloud-storage`.

## How the pieces interact

- `main.py` is the only externally invoked component (HTTP). Everything else is a library function it pulls in through `full_process()`.
- `full_process.py` is the conductor — it owns configuration (env vars) and the order of operations, but delegates all real work to the other three modules.
- `get_last_token.py` and `changes_to_bq()` (in `get_changes.py`) form the state loop: the token read at the start of a run is the token that was written at the end of the previous run, so each invocation only processes changes that happened since last time.
- `get_changes.py` is the Drive-facing read side; `move_file.py` is the Drive-read / GCS-write side. They don't call each other — `full_process()` feeds the `fileid` produced by `get_changes.py` into `move_file.py`.

## Authentication

All Google clients rely on Application Default Credentials (`google.auth.default()` / default `storage.Client()` / `bigquery.Client()`), so the runtime service account must have access to the Drive, the GCS bucket, and BigQuery.

> Note: `drive_bucket_test.ipynb` is a scratch/testing notebook and is intentionally not documented here.
