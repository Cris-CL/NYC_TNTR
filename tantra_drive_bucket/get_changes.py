import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from datetime import datetime
import pytz



def fetch_changes(saved_start_page_token,creds=None):
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
    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)

        # Begin with our last saved start token for this user or the
        # current token from getStartPageToken()
        page_token = saved_start_page_token
        # pylint: disable=maybe-no-member

        while page_token is not None:
            response = service.changes().list(pageToken=page_token,
                                              spaces='drive').execute()
            for change in response.get('changes'):
                # Process change
                print(F'Change found for file: {change.get("fileId")}')
            if 'newStartPageToken' in response:
                # Last page, save this token for the next polling interval
                saved_start_page_token = response.get('newStartPageToken')
            page_token = response.get('nextPageToken')

    except HttpError as error:
        print(F'An error occurred: {error}')
        saved_start_page_token = None

    return saved_start_page_token, response

def changes_to_df(token,response_json):
  try:
      df = pd.json_normalize(response_json["changes"])
      df = df[["fileId","time","file.name"]]
      df["token"] =  token
      df.columns = [col.lower().replace(".","_") for col in df.columns]
      df = df.astype({"token":"int32"})
      df['time'] = pd.to_datetime(df['time']).dt.tz_convert(None)
  except Exception as e:
    print("Probably there is no new data")
    print(f"Error: {e}")
    return False
  return df.copy()

def df_to_dict(df):
  result_dict = df.set_index('file_name')['fileid'].to_dict()
  return result_dict

def changes_to_bq(df,dataset,tracker_table):
  table_name = f"{dataset}.{tracker_table}"
  df.to_gbq(
              destination_table=table_name,
              progress_bar=False,
              if_exists="append")
  return
