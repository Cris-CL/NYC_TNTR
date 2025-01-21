# pandas==1.5.1
# google-cloud-bigquery>=3.3.5
# gspread==5.7.2

import os
import pandas as pd
import google.auth
from google.cloud import bigquery
from new_process import *
from time import sleep
from new_query import create_new_query
from sheets_util import *
import datetime
import json
import requests
from google.cloud import storage


def create_retry_message(attempt,year,month,names_list):
    message = {
        "type": "retry",
        "attempt": attempt,
        "year": year,
        "month": month,
        "names": names_list,
    }
    return message


def write_failed_sheets_to_json(names, year, month, attempt=0):

    bucket_name = os.environ["BUCKET_RETRY"]
    today_date = datetime.date.today().strftime("%Y_%m_%d")
    file_name = f"failed_process_{today_date}_{year}{month}.json"
    current_attempt = attempt + 1

    failed_sheets = create_retry_message(current_attempt, year, month, names)

    headers = {"Content-Type": "application/json"}
    TEST_FUNCTION = os.environ["TEST_FUNCTION"]
    try:
        print("Sending request to test function url")
        requests.post(TEST_FUNCTION, json=failed_sheets, headers=headers)
    except Exception as e:
        print(e)
    save_json_to_bucket(bucket_name, file_name, failed_sheets)
    return


def save_json_to_bucket(bucket_name, file_name, data):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(data, indent=2))
    return


def get_dataframe(month=9, year=2023):
    QUERY_ASSIS = create_new_query(month, year)
    # Initialize Google Sheets and BigQuery clients

    # BigQuery query
    query = QUERY_ASSIS
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )
    # Create a BigQuery client with the obtained credentials
    bq_client = bigquery.Client(credentials=credentials)

    query_job = bq_client.query(query)
    results_df = query_job.to_dataframe()

    # Convert time columns to string

    for col in results_df.columns:
        results_df[col] = results_df[col].astype(str)
        results_df[col] = results_df[col].map(lambda x: None if isinstance(x,str) and x.lower() in ('nan', '<na>','0.0') else x)
    return results_df


def process_sheets_from_master(month, year_process, host_names="All", attempts=1):
    print(f"Start processing for the year: {year_process} and the month: {month}")
    spread_2023 = os.environ["MASTER_SPREADSHEET_ID"]
    spread_2024 = os.environ["MASTER_SPREADSHEET_ID_24"]
    spread_2025 = os.environ["MASTER_SPREADSHEET_ID_25"]

    master_selector = {
        "2023": spread_2023,
        "2024": spread_2024,
        "2025": spread_2025
    }
    master_id = master_selector[str(year_process)]

    hostess_dict = get_hostess_dict(master_id)
    if isinstance(host_names, list):
        try:
            lis_names = host_names
            hostess_dict = {
                name_sing: hostess_dict.get(name_sing, "") for name_sing in lis_names
            }
        except Exception as e:
            print("Couldn't process individual names")
            print(e, type(e))

    results_df = get_dataframe(month=month, year=year_process)
    try:
        names_in_df = results_df["hostess_name"].unique().tolist()
        hostess_dict = {
            k: hostess_dict[k] for k in names_in_df if k in hostess_dict.keys()
        }
    except Exception as e:
        print(f"Error while getting current df names: {e}")
        hostess_dict = get_hostess_dict("MASTER_SPREADSHEET_ID")

    if len(results_df) < 1:
        print(f"No new data for the current month: {month} ")
        return []
    names_not_updated = new_updater(results_df, hostess_dict, year_process, month)

    if isinstance(names_not_updated, list) and len(names_not_updated) > 0 and attempts > 1:
        print("Some updates failed, sending retry notice")
        write_failed_sheets_to_json(
            names_not_updated, year_process, month, attempt=attempts
        )
    if isinstance(host_names, list):
        print(
            f"Finished processing {','.join(host_names)} hostess for the month {month}"
        )
    elif isinstance(host_names, str) and len(names_not_updated) == 0:
        print(f"Finished processing all hostess for the month {month}")
    elif isinstance(host_names, str) and len(names_not_updated) > 0:
        print(
            f"Finished processing all hostess except {','.join(names_not_updated)} for the month {month}"
        )
    return names_not_updated
