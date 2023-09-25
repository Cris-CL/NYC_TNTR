import os
from dotenv import load_dotenv
import uuid
from datetime import datetime
from googleapiclient.discovery import build

load_dotenv()
def setup_drive_webhook(drive_folder_id,credentials,min_factor=1,week_factor=0,day_factor = 0):

    PROJECT_ID = os.environ.get("PROJECT_ID")
    function_name = os.environ.get("FUNCTION_NAME")
    drive_service = build('drive', 'v3',credentials=credentials,)

    subscription_url = f"https://us-central1-{PROJECT_ID}.cloudfunctions.net/{function_name}"
    print(subscription_url)
    #### this is the timestamp of the current time in miliseconds
    miliseconds = 1000
    seconds = 1 * miliseconds
    minutes = 60 * seconds
    hours = 60 * minutes
    days = 24 * hours
    weeks = 7 * days

    now = int(datetime.utcnow().timestamp()*1e3)
    print(now)
    expiration = now + min_factor * minutes +9*hours + weeks * week_factor + days * day_factor # 9 hours to account for time zone difference

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
