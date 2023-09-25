import os
from get_last_token import get_token
from get_changes import fetch_changes,changes_to_df,df_to_dict,changes_to_bq
from move_file import transfer_file_between_drive_and_gcs

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET = os.environ.get("DATASET")
TABLE_NAME = os.environ.get("TABLE_NAME")
FOLDER_ID = os.environ.get("FOLDER_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

def full_process():
  print("Starting")
  ## Get and process new changes
  last_token = get_token(PROJECT_ID,DATASET,TABLE_NAME) ## get token from database

  new_token,changes = fetch_changes(last_token) ## get new changes from last time

  df_changes = changes_to_df(new_token,changes) ## Put the changes info in a dataframe

  if isinstance(df_changes,bool) == True:
    print("There is no new data")
    return

  dict_changes = df_to_dict(df_changes) ## make a dict with the file names and file id for looping later

  ## Move files from the drive folder to the bucket
  for file_name in dict_changes.keys():
    if isinstance(file_name, str) and '.xlsx' in file_name:
      file_id = dict_changes[file_name]
      print(f"Moving {file_name} id: {file_id}")
      try:
        transfer_file_between_drive_and_gcs(file_id,BUCKET_NAME,file_name)
      except Exception as e:
        print(e)
  changes_to_bq(df_changes,DATASET,TABLE_NAME)
  return
