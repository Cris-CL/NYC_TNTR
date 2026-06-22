# webhook_tantra

The **public webhook listener** that Google Drive calls whenever the watched shared folder changes. It is the entry point of the Drive → bucket → BigQuery pipeline: it does almost no work itself, it just decides whether a notification is worth acting on and, if so, triggers the `tantra_drive_bucket` function.

## Where this fits in the overall pipeline

```
wh_listener_scheduler   ── registers/renews the Drive files().watch on the folder
        │  (the watch makes Drive POST here on every change)
        ▼
webhook_tantra (this)   ── receives Drive's notification, filters out noise,
        │                   fire-and-forget POST to the drive-bucket function
        ▼
tantra_drive_bucket     ── pulls the changes, moves .xlsx files to the stage bucket
        ▼
tantra_msg_send         ── on new bucket object, publishes a Pub/Sub message
        ▼
(downstream consumer)   ── reads file from bucket, validates, loads new version to BigQuery
```

## Files

### `main.py` — Webhook listener (Cloud Function)
A Flask handler on `/webhook`.

- `GET` → returns a small HTML string (health/verification check).
- `POST` → this is the notification Google Drive sends for the watched folder. It:
  1. Logs the request time in both GMT and Tokyo time, plus the raw headers.
  2. Reads the Drive resource-state header (`posted_data[4][1]`, i.e. the 5th header — Google's `X-Goog-Resource-State`).
  3. **Ignores `sync` and `trash` notifications** — returns success immediately, because the initial `sync` handshake and trash events are not real content additions.
  4. For any other state, **fires a `POST` to `DRIVE_FUNCTION_URL`** (the `tantra_drive_bucket` function) with a **1-second timeout**. The timeout is intentional fire-and-forget: it kicks off the heavier downstream function without waiting for it, and the expected `ReadTimeout` is caught and treated as "request sent."

- `get_timestamp(timezone_name)` — helper that formats the current time in a given timezone.

Environment variables: `DRIVE_FUNCTION_URL` (the URL of the `tantra_drive_bucket` function to trigger).

## Notes
- The 1s-timeout pattern means this listener returns quickly to Drive (Drive expects a fast 200 or it retries/penalizes the channel) while the slow work happens in the triggered function.
- Header position parsing (`posted_data[4]`) is positional and therefore fragile — it assumes Google keeps `X-Goog-Resource-State` as the 5th header. See [tantra_drive_bucket/README.md](../tantra_drive_bucket/README.md) for the function that does the actual file processing.
