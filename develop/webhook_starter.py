import os
from datetime import datetime
from googleapiclient.discovery import build

def setup_drive_webhook(drive_folder_id, topic_name,credentials):

    PROJECT_ID = os.environ.get("PROJECT_ID")
    function_name = os.environ.get("FUNCTION_NAME")
    drive_service = build('drive', 'v3',credentials=credentials,)

    subscription_url = f"https://us-central1-{PROJECT_ID}.cloudfunctions.net/{function_name}"
    print(subscription_url)
    #### this is the timestamp of the current time in miliseconds
    now = int(datetime.utcnow().timestamp()*1e3)
    expiration = now + 604800000
    # Create a watch request
    id_gen = str(uuid.uuid4())
    print(id_gen)
    watch_request = {
        'id': id_gen,
        'type': 'web_hook',
        'address': subscription_url,
        'expiration': expiration,
    }
    drive_service.files().watch(fileId=drive_folder_id, body=watch_request).execute()
    return id_gen
