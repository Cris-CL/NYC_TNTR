### I need a script to take a file from google drive and put it in a bucket, i will use the file id to download it and then put it in the bucket
import io
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage

def transfer_file_between_drive_and_gcs(
    drive_credentials_file,
    drive_file_id,
    gcs_bucket_name,
    gcs_object_name
):
    try:
        # Initialize Google Drive API client
        drive_service = build('drive', 'v3', credentials=drive_credentials_file)

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
        bucket = gcs_client.get_bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_object_name)

        drive_file.seek(0)
        blob.upload_from_file(drive_file)

        print(f"File {gcs_object_name} uploaded to {gcs_bucket_name} successfully.")
        return True
    except Exception as e:
        print(f"Error transferring file: {str(e)}")
        return False

# # Example usage
# drive_credentials_file = 'drive_credentials.json'
# drive_file_id = 'YOUR_DRIVE_FILE_ID'
# gcs_credentials_file = 'gcs_credentials.json'
# gcs_bucket_name = 'your-gcs-bucket'
# gcs_object_name = 'destination/file/name.ext'

# if transfer_file_between_drive_and_gcs(
#     drive_credentials_file,
#     drive_file_id,
#     gcs_credentials_file,
#     gcs_bucket_name,
#     gcs_object_name
# ):
#     print(f"File transferred successfully to Google Cloud Storage.")
# else:
#     print(f"Failed to transfer the file.")
