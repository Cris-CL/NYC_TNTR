# tantra_msg_send

A **Cloud Storage–triggered** Cloud Function that fires whenever a new object lands in the stage bucket and **publishes a Pub/Sub message** announcing that file. It is the bridge between "a file was moved into the bucket" and the downstream consumer that reads the file, validates it, and loads a new version into BigQuery.

## Where this fits in the overall pipeline

```
tantra_drive_bucket  ── moves an .xlsx into the stage bucket (object finalize)
        │
        ▼
tantra_msg_send (this)  ── triggered by the bucket object-finalize event,
        │                   publishes {file_name, file_uri} to a Pub/Sub topic
        ▼
(downstream consumer)   ── subscribed to the topic; reads gs:// file, checks the
                           contents/naming fit the target table, loads new version to BigQuery
```

## Files

### `main.py` — Bucket-event → Pub/Sub publisher (Cloud Function)

- `send_message(event, context)` — the entry point, using the Cloud Functions **GCS background-event** signature (`event, context`):
  - `event['bucket']` and `event['name']` give the bucket and object name of the file that triggered the event.
  - Builds the GCS URI `gs://{bucket}/{name}`.
  - Constructs a JSON message `{"file_name": ..., "file_uri": ...}`, encodes it to UTF-8, and publishes it to `projects/{PROJECT_NAME}/topics/{TOPIC_NAME}`.
  - Errors during publish are caught and logged.

Module-level clients (`storage.Client()`, `pubsub_v1.PublisherClient()`) are created once at cold start and reused.

Environment variables: `PROJECT_NAME`, `TOPIC_NAME` (the Pub/Sub topic the downstream consumer subscribes to).

## Notes
- Because this is triggered by the bucket's **object-finalize** event, it fires on every upload **including overwrites** — so replacing a file's contents in the bucket re-announces it downstream. This makes it more reliable than the upstream Drive change feed for kicking off processing. See [tantra_drive_bucket/README.md](../tantra_drive_bucket/README.md) for how files arrive in the bucket.
- The `storage_client` is imported/created but not actively used in the publish path; the function only needs the event metadata and the Pub/Sub publisher.
