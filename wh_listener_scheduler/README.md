# wh_listener_scheduler

A small scheduled Cloud Function whose only job is to **(re)register the Google Drive watch channel** on the shared folder. Drive `files().watch` subscriptions expire, so this function must run periodically (via Cloud Scheduler → Pub/Sub) to keep the `webhook_tantra` listener receiving change notifications. Without it, the watch lapses and no Drive changes reach the pipeline.

## Where this fits in the overall pipeline

```
wh_listener_scheduler (this)  ── periodically calls Drive files().watch on the folder
        │                          so Drive keeps POSTing change notifications
        ▼
webhook_tantra                ── receives those notifications
        ▼
tantra_drive_bucket → tantra_msg_send → (downstream → BigQuery)
```

## Files

### `main.py` — Watch registration (Cloud Function)

- `setup_shared_folder_webhook(shared_folder_id, week_factor=0, test_var=True, day_factor=1)`
  - Picks the target function name from env: `WH_DUMMY` when `test_var=True`, `FUNCTION_NAME` when `test_var=False`. This selects whether the watch points at the test listener or the production listener.
  - Builds the listener URL as `https://us-central1-{PROJECT_ID}.cloudfunctions.net/{function_name}`.
  - Computes an `expiration` timestamp (in epoch milliseconds) from `week_factor`, `day_factor`, and a fixed `h_factor` hours offset (the hours offset compensates for timezone difference). This sets how long the watch stays alive.
  - Generates a random channel `id` (`uuid4`) and issues `drive_service.files().watch(...)` with `supportsAllDrives=True` so it works on shared drives.

- `starter(data, context)` — the **entry point** invoked by the Pub/Sub/Scheduler trigger (`data, context` is the Cloud Functions background-event signature). It calls `setup_shared_folder_webhook` with `day_factor=2`, `test_var=False` — i.e. register a **production** watch that lasts ~2 days. Re-running before expiry keeps the channel alive.

Environment variables: `SHARE_FOLDER_ID` (folder to watch), `PROJECT_ID`, `FUNCTION_NAME` (prod listener), `WH_DUMMY` (test listener).

Authentication: Application Default Credentials (`google.auth.default()`).

## Notes
- The schedule interval must be shorter than the watch lifetime (`day_factor=2` → ~2 days) so the channel never lapses between runs.
- Each run creates a **new** channel id; old channels expire on their own (or can be stopped via `channels().stop`, as seen in the drive-bucket test notebook).
- The listener this points at is documented in [webhook_tantra/README.md](../webhook_tantra/README.md).
