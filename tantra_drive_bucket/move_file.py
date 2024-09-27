import io
from googleapiclient.discovery import build
from google.oauth2 import service_account
import google.auth
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage


def check_bucket(bucket_name, file_name):
    """
    Function that given a bucket_name returns true if there is a file named
    file_name inside or false if is not.

    Parameters:
    - bucket_name (String) : Name of the bucket to check
    - file_name (String) : Name of the file we want to know if is in the bucket

    Returns:
    - Boolean: True if the filename is contained in the bucket, False otherwise
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        list_names = []
        for blob_data in bucket.list_blobs():
            list_names.append(blob_data.name)
        return file_name in list_names
    except:
        return False


def transfer_file_between_drive_and_gcs(
    drive_file_id, gcs_bucket_name, gcs_object_name, creds=None
):
    if not creds:
        creds, _ = google.auth.default()
    try:
        # Initialize Google Drive API client
        drive_service = build("drive", "v3", credentials=creds)

        # Initialize Google Cloud Storage client
        gcs_client = storage.Client()

        # Download the file from Google Drive
        request = drive_service.files().get_media(fileId=drive_file_id)
        drive_file = io.BytesIO()
        downloader = MediaIoBaseDownload(drive_file, request)
        done = False

        while not done:
            status, done = downloader.next_chunk()

        # Upload the file to Google Cloud Storage
        if check_bucket(gcs_bucket_name, gcs_object_name):
            print(f"File: {gcs_object_name} Already  in {gcs_bucket_name} bucket")
            return False
        bucket = gcs_client.get_bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_object_name)

        drive_file.seek(0)
        blob.upload_from_file(drive_file)

        print(f"File {gcs_object_name} uploaded to {gcs_bucket_name} successfully.")
        return True
    except Exception as e:
        print(f"Error transferring file: {str(e)}")
        return False
