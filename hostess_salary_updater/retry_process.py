from google.cloud import storage
import json

def get_retry_files(bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    try:
        # messages = [json.loads(file.download_as_text()) for file in bucket.list_blobs()]
        files = [file for file in bucket.list_blobs()]
    except Exception as e:
        print(e)
        files = []
    return files

def delete_blob(blob):
    try:
        blob.delete()
    except Exception as e:
        print(e, "Couldn't delete blob")
    return

def blob_to_message(blob):
    message = json.loads(blob.download_as_text())
    return message
