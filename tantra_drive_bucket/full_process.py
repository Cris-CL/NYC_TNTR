import os
from get_last_token import get_token
from get_changes import fetch_changes,changes_to_df,df_to_dict,changes_to_bq,fetch_changes_specific
from move_file import transfer_file_between_drive_and_gcs
from datetime import datetime

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET = os.environ.get("DATASET")
TABLE_NAME = os.environ.get("TABLE_NAME")
FOLDER_ID = os.environ.get("FOLDER_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

#### NEW SHARED FOLDER VAR
DRIVE_ID = os.environ.get("DRIVE_ID")
FOLDER_ID_2 = os.environ.get("FOLDER_ID_2")


def full_process():
  print("Starting")
  ## Get and process new changes
  last_token = get_token(PROJECT_ID,DATASET,TABLE_NAME) ## get token from database

  # new_token,changes = fetch_changes(last_token) ## get new changes from last time
  new_token,changes = fetch_changes_specific(saved_start_page_token=last_token,drive_id=DRIVE_ID) ## get new changes from last time

  df_changes = changes_to_df(new_token,changes) ## Put the changes info in a dataframe
  df_changes.sort_values(by='time', inplace=True, ascending=True) ## Sort the dataframe in ascending order by the time of modification

  if isinstance(df_changes,bool) == True:
    print("There is no new data")
    return

  dict_changes = df_to_dict(df_changes) ## make a dict with the file names and file id for looping later

  ## Move files from the drive folder to the bucket
  today_date = datetime.now().date()
  for index, row in df_changes.iterrows():
    file_name = row['file_name']
    file_id = row['fileid']
    try:
      time_stamp = row['time'].date()
      if time_stamp != today_date:
        continue
    except Exception as e:
      print(e)

    print(f'File Name: {file_name}, File ID: {file_id}')
    if isinstance(file_name, str) and '.xlsx' in file_name:
      print(f"Moving {file_name} id: {file_id}")
      try:
        transfer_file_between_drive_and_gcs(file_id,BUCKET_NAME,file_name)
      except Exception as e:
        print(e)

  # for file_name in dict_changes.keys():
  #   if isinstance(file_name, str) and '.xlsx' in file_name:
  #     file_id = dict_changes[file_name]
  #     print(f"Moving {file_name} id: {file_id}")
  #     try:
  #       transfer_file_between_drive_and_gcs(file_id,BUCKET_NAME,file_name)
  #     except Exception as e:
  #       print(e)

  changes_to_bq(df_changes,DATASET,TABLE_NAME)
  return
