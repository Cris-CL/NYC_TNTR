# tantra_drive_bucket — Robustness Checklist (future development)

A prioritized list of changes to make the Drive → bucket function reliable. Background: the function currently discovers files via the Drive **change feed + stored token**, which is best-effort and silently dropped real `.xlsx` uploads (e.g. `NYC_20260617_*`). Both the webhook ping and the scheduled fallback funnel into this same discovery path, so neither catches a missed *event*. The core fix is to change *how files are discovered*, then harden the surrounding logic.

---

## P0 — Core reliability (the actual gap)

- [ ] **Switch discovery from the change feed to folder reconciliation.**
  - How: replace `fetch_changes_specific` as the source of truth with `files().list` on the target folder (`q="'<FOLDER_ID>' in parents and trashed=false ..."`, `corpora="drive"`, `driveId`, `includeItemsFromAllDrives=True`, `supportsAllDrives=True`). List the folder's *current* contents each run instead of asking "what changed."
  - Why: immune to dropped Drive events — the file is found because it's *in the folder*, not because Drive emitted a change.

- [ ] **Key processing state on `(fileId, md5Checksum)`.**
  - How: request `fields="files(id,name,md5Checksum,createdTime,modifiedTime,parents,size)"`. A file is "to ingest" if its `fileId` is unknown **or** its `md5Checksum` differs from the last ingested one.
  - Why: covers both real cases — fresh upload (new `fileId`) and in-place modification (same `fileId`, new checksum). `fileId` alone would silently skip modified files.

- [ ] **Record ingestion *outcome*, not just "seen".**
  - How: add state columns: `status` (success/failed/skipped), `ingested_at`, `gcs_object_name`, `error`. Persist after the GCS upload succeeds.
  - Why: today a tracker row means "a change was seen," not "the file was delivered." Outcome state lets failed files retry on the next run instead of being lost.

- [ ] **Remove the `today_date` filter.**
  - How: delete the `if time_stamp != today_date: continue` branch in `full_process`. Replace the condition with "not yet successfully ingested."
  - Why: it silently skips files when processing lags past midnight or across timezones (and would have skipped the lost files even if discovery worked).

---

## P1 — Correctness bugs found this session

- [ ] **Fix the bucket "skip if name exists" logic.**
  - Where: [move_file.py:54](move_file.py#L54) (`check_bucket` → early `return False`).
  - How: upload (overwrite) when the source `md5Checksum` differs from what's recorded; only skip when checksum matches. GCS overwrite to the same object name is atomic — no manual delete needed unless the name changes.
  - Why: for a modified file the name already exists, so the current code wrongly skips the new version.

- [ ] **Fix `.xlsx` vs native Google Sheets detection.**
  - Where: name filter in [get_changes.py:74-78](get_changes.py#L74-L78) and the loop in `full_process`.
  - How: match on **mimeType** (`application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` for real `.xlsx`) rather than the `"xlsx" in name` string. If native Sheets ever need handling, they require `files().export_media()` (export to xlsx), since `get_media()` can't download Google-native formats.
  - Why: the current string match is brittle, and `get_media` silently fails on native Sheets.

- [ ] **Fix `changes_to_df` return-type inconsistency.**
  - Where: [get_changes.py:52-82](get_changes.py#L52-L82) returns `False` in one path and an empty DataFrame in others.
  - How: always return a DataFrame (possibly empty); have callers check `len(df)`. Currently `full_process` calls `len(df_changes)` which would crash on a `False` return.

- [ ] **Guard the `None` token / empty-table case.**
  - Where: `astype({"token": "int32"})` in `changes_to_df` and `get_token` returning `None`.
  - How: if discovery moves to `files().list`, the token stops being load-bearing; otherwise seed a start token when the table is empty and never cast `None` to int.

- [ ] **Fix the multi-page accumulation bug (if any change-feed code is retained).**
  - Where: [get_changes.py:120-145](get_changes.py#L120-L145) — `fetch_changes_specific` overwrites `response` per page and returns only the last page.
  - How: accumulate `response.get("changes", [])` across all pages. (Only relevant if the change feed is kept for anything; P0 removes it from the critical path.)

---

## P2 — Idempotency & data integrity

- [ ] **Make BigQuery writes idempotent (upsert, not blind append).**
  - How: use `MERGE` on `fileId` for the state table instead of `to_gbq(if_exists="append")`, so re-runs don't accumulate duplicate rows (the current tracker has many rows per file).
  - Why: reconciliation re-lists every run; append would duplicate state endlessly.

- [ ] **Propagate a stable key + version into the downstream BigQuery load.**
  - How: carry `source_file_id` + `source_md5` (or date+type key) so the downstream consumer can **replace** a file's rows on modification rather than appending duplicate data.
  - Why: "modify → delete old → upload new" must reach BigQuery as a replace, end-to-end.

- [ ] **Decide the role of the token / tracker table.**
  - How: keep it as an append-only audit log if useful, but stop letting discovery depend on it. Document that state-of-truth is the `fileId`-keyed table.

---

## P3 — Observability & operations

- [ ] **Replace bare `except: / except Exception` swallowing with real logging.**
  - Where: throughout `full_process`, `move_file`, `changes_to_df` — many errors are printed and execution continues silently.
  - How: structured logs (Cloud Logging), and surface per-file failures into the `status`/`error` state columns.

- [ ] **Add an alert for "expected files missing."**
  - How: if a reconciliation run finds 0 new files when files were expected (or a file sits un-ingested for > N hours), log a warning / fire an alert. The original failure was invisible because "no changes" looked normal.

- [ ] **Emit a reconciliation summary per run** (files seen, ingested, skipped, failed) for traceability.

- [ ] **Confirm timezone handling** is consistent (Drive returns UTC; current code mixes `datetime.now()` local with tz-naive conversions).

---

## P4 — Testing & hardening

- [ ] **Unit-test the discovery + dedup logic** against fixtures (new file, modified file, unchanged file, trashed file, native Sheet vs xlsx).
- [ ] **Add an end-to-end check**: drop a known file in a test folder → assert it lands in the bucket and the state row is written.
- [ ] **Pin and review dependencies** (the deployed pins are old — pandas 1.5.1, etc.); validate before upgrading.

---

## Summary of the single most important change
Everything above is secondary to **P0: discover by reconciling the folder (`files().list`) keyed on `fileId` + `md5Checksum`, recording ingestion outcome.** That one change closes the dropped-event gap for *both* the webhook and the scheduled-fallback trigger at once. See [pipeline.md](../pipeline.md) and [README.md](README.md) for context.
