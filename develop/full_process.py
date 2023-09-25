import os
from get_last_token import get_token
from get_changes import fetch_changes,changes_to_df,df_to_dict,changes_to_bq
from move_file import transfer_file_between_drive_and_gcs

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET = os.environ.get("DATASET")
TABLE_NAME = os.environ.get("TABLE_NAME")
FOLDER_ID = os.environ.get("FOLDER_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
TRACKER_TABLE = os.environ.get("TRACKER_TABLE")

def full_process():
  ## Get and process new changes
  last_token = get_last_token(PROJECT_ID,DATASET,TABLE_NAME)

  new_token,changes = fetch_changes(last_token)

  df_changes = changes_to_bq(new_token,changes)

  dict_changes = df_to_dict(df_changes)

  ## Move files from the drive folder to the bucket
  for file_name in dict_changes.keys():
    if isinstance(file, str) and '.xlsx' in file:
      file_id = dict_changes[file_name]
      print(f"Moving {file_name} id: {file_id}")
      try:
        transfer_file_between_drive_and_gcs(file_id,BUCKET_NAME,file_name)
      except Exception as e:
        print(e)

  changes_to_bq(df_changes,DATASET,TRACKER_TABLE,FOLDER_ID)

  return
