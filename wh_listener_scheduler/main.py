import os
import uuid
from datetime import datetime
import google.auth
from googleapiclient.discovery import build

SHARE_FOLDER_ID = os.environ.get("SHARE_FOLDER_ID")


def setup_shared_folder_webhook(shared_folder_id, week_factor=0, test_var=True,day_factor=1):

    PROJECT_ID = os.environ.get("PROJECT_ID")
    if test_var == True:
        function_name = os.environ.get("WH_DUMMY")
    if test_var == False:
        function_name = os.environ.get("FUNCTION_NAME")

    credentials, _ = google.auth.default()

    drive_service = build('drive', 'v3', credentials=credentials)

    subscription_url = f"https://us-central1-{PROJECT_ID}.cloudfunctions.net/{function_name}"
    print(subscription_url)

    miliseconds = 1000
    seconds = 1 * miliseconds
    minutes = 60 * seconds
    hours = 60 * minutes
    days = 24 * hours
    weeks = 7 * days

    #### this is the timestamp of the current time in miliseconds
    now = int(datetime.utcnow().timestamp()*1e3)
    # print(now)
    h_factor = 7
    expiration = now + weeks * week_factor - 1 * minutes + day_factor*days + hours*h_factor # 9 hours to account for time zone difference
    # print(expiration)

    # Create a watch request
    id_gen = str(uuid.uuid4())
    print(id_gen)
    watch_request = {
        'id': id_gen,
        'type': 'web_hook',
        'address': subscription_url,
        'expiration': expiration,
    }
    drive_service.files().watch(fileId=shared_folder_id, body=watch_request,supportsAllDrives=True,).execute()

    return

def starter(data, context):
    setup_shared_folder_webhook(shared_folder_id=SHARE_FOLDER_ID,week_factor=0,day_factor=2,test_var=False)
    return
