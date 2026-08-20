import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from datetime import datetime
import pytz


def get_fresh_start_token(service, drive_id=None):
    """Ask Drive for a fresh startPageToken (baseline = now)."""
    params = {}
    if drive_id:
        params = {"driveId": drive_id, "supportsAllDrives": True}
    response = service.changes().getStartPageToken(**params).execute()
    return response.get("startPageToken")


def fetch_changes(saved_start_page_token, creds=None):
    """Retrieve the list of changes for the currently authenticated user.
        prints changed file's ID
    Args:
        saved_start_page_token : StartPageToken for the current state of the
        account.
    Returns: saved start page token.

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    print("fetching changes")
    if not creds:
        creds, _ = google.auth.default()
    response = None
    # create drive api client
    service = build("drive", "v3", credentials=creds)
    try:
        if saved_start_page_token is None:
            print("No saved token, starting from a fresh start page token")
            saved_start_page_token = get_fresh_start_token(service)

        # Begin with our last saved start token for this user or the
        # current token from getStartPageToken()
        page_token = saved_start_page_token
        # pylint: disable=maybe-no-member

        while page_token is not None:
            response = (
                service.changes().list(pageToken=page_token, spaces="drive").execute()
            )
            # for change in response.get('changes'):
            # Process change
            # print(F'Change found for file: {change.get("fileId")}')
            if "newStartPageToken" in response:
                # Last page, save this token for the next polling interval
                saved_start_page_token = response.get("newStartPageToken")
            page_token = response.get("nextPageToken")

    except HttpError as error:
        print(f"An error occurred (status {error.resp.status}): {error}")
        if error.resp.status == 404:
            # Drive only keeps change history for a limited window; a 404
            # means the saved token expired, so reset the baseline to now
            try:
                saved_start_page_token = get_fresh_start_token(service)
                print(f"Saved token expired, reset to {saved_start_page_token}")
            except HttpError as reset_error:
                print(f"Could not reset start page token: {reset_error}")
                saved_start_page_token = None
        else:
            saved_start_page_token = None

    return saved_start_page_token, response


def changes_to_df(token, response_json):
    df = pd.DataFrame()
    try:
        df = pd.json_normalize(response_json["changes"])
        df = df[df["removed"] == False]
        if len(df) == 0:
            print("No new data")
            return False
    except Exception as e:
        print("Probably there is no new data")
        print(f"Error: {e}")
    try:
        df = df[["fileId", "time", "file.name"]]
        df["token"] = token
        df.columns = [col.lower().replace(".", "_") for col in df.columns]
        df = df.astype({"token": "int32"})
        df["time"] = pd.to_datetime(df["time"]).dt.tz_convert(None)
    except Exception as e:
        print("Dataframe doesnt have the desired colums")
        print(f"Dataframe columns {' '.join(df.columns)}")
    df_orig = df.copy()
    try:
        df = df[
            df["file_name"].map(
                lambda x: True if isinstance(x, str) and "xlsx" in x else False
            )
        ].copy()
    except Exception as e:
        print(f"Error when filtering df for xlsx: {e}")
        df = df_orig
    return df


def df_to_dict(df):
    result_dict = df.set_index("file_name")["fileid"].to_dict()
    return result_dict


def changes_to_bq(df, dataset, tracker_table):
    table_name = f"{dataset}.{tracker_table}"
    df.to_gbq(destination_table=table_name, progress_bar=False, if_exists="append")
    return


def fetch_changes_specific(saved_start_page_token, drive_id, creds=None):
    """Retrieve the list of changes for the currently authenticated user.
        prints changed file's ID
    Args:
        saved_start_page_token : StartPageToken for the current state of the
        account.
    Returns: saved start page token.

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    print("fetching changes")
    if not creds:
        creds, _ = google.auth.default()
    response = None
    # create drive api client
    service = build("drive", "v3", credentials=creds)
    try:
        if saved_start_page_token is None:
            print("No saved token, starting from a fresh start page token")
            saved_start_page_token = get_fresh_start_token(service, drive_id)

        # Begin with our last saved start token for this user or the
        # current token from getStartPageToken()
        page_token = saved_start_page_token
        # print(f"Page token: {page_token}")
        # pylint: disable=maybe-no-member
        while page_token is not None:
            print("starting loop")
            response = (
                service.changes()
                .list(
                    pageToken=page_token,
                    spaces="drive",
                    driveId=drive_id,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                )
                .execute()
            )
            # for change in response.get('changes'):
            # Process change
            # print(f'Change found for file: {change.get("file",{}).get("name","Unknown")}')
            if "newStartPageToken" in response:
                # Last page, save this token for the next polling interval
                saved_start_page_token = response.get("newStartPageToken")
            page_token = response.get("nextPageToken")

    except HttpError as error:
        print(f"An error occurred (status {error.resp.status}): {error}")
        if error.resp.status == 404:
            # Drive only keeps change history for a limited window; a 404
            # means the saved token expired, so reset the baseline to now
            try:
                saved_start_page_token = get_fresh_start_token(service, drive_id)
                print(f"Saved token expired, reset to {saved_start_page_token}")
            except HttpError as reset_error:
                print(f"Could not reset start page token: {reset_error}")
                saved_start_page_token = None
        else:
            saved_start_page_token = None
    print("Finished loop")
    return saved_start_page_token, response
