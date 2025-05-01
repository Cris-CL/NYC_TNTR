import os
import google.auth
from updater_process import updater
from query_generator import create_new_query
from get_spread_info import get_hostess_dict
import datetime
import json
import requests
from google.cloud import bigquery
from google.cloud import storage


def create_retry_message(attempt, year, month, names_list):
    """
    Function that builds the retry message in case of failure.

    Args:
        attempt (int): Number of the current attempt.
        year (int): Year of the current attempt.
        month (int): Month of the current attempt.
        names_list (list): List with the names of the failed

    Returns:
        message (dict): Formatted arguments for the retry message.
    """

    message = {
        "type": "retry",
        "attempt": attempt,
        "year": year,
        "month": month,
        "names": names_list,
    }
    return message


def write_failed_sheets_to_json(names, year, month, attempt=0):
    """
    Function that creates a json file with the message with failed hostess sheets
    for a later retry.

    Args:
        attempt (int): Number of the current attempt.
        year (int): Year of the current attempt.
        month (int): Month of the current attempt.
        names_list (list): List with the names of the failed
    """

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
        print(f"Error in write_failed_sheets_to_json {e} ---- {type(e)}.")
    save_json_to_bucket(bucket_name, file_name, failed_sheets)
    return


def save_json_to_bucket(bucket_name, file_name, data):
    """
    Function that saves a file to a bucket.

    Args:
        bucket_name (str): Name of the bucket where the data will be saved.
        file_name (str): Name of the new file.
        data (str): Content of the file.
    """

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(data, indent=2))
    return


def get_dataframe(month=9, year=2023):
    """
    Function that gets the full dataframe with all of the data from the specified
    month and year.

    Args:
        year (int): Year of the current attempt.
        month (int): Month of the current attempt.

    Returns:
        results_df (pd.Dataframe): Dataframe with the full data from the query result.
    """
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

    try:
        query_job = bq_client.query(query)
        results_df = query_job.to_dataframe()
    except Exception as e:
        print("Error in get_dataframe query_job part")
        raise e

    # Convert time columns to string

    for col in results_df.columns:
        results_df[col] = results_df[col].astype(str)
        results_df[col] = results_df[col].map(
            lambda x: (
                None
                if isinstance(x, str) and x.lower() in ("nan", "<na>", "0.0")
                else x
            )
        )
    return results_df


def process_sheets_from_master(month, year_process, host_names="All", attempts=1):
    """
    Function that excecutes the process of updating all the hostess sheets.

    Args:
        month (int): Month to be updated.
        year_process (int): Year to be updated.
        host_names (int/): Hostess names to update, in case is a retry, this var
        will be a list with the names.
        attempts (int): Year of the current attempt.

    Returns:
        names_not_updated (list): List of names where some error happened and
        couldnt update, in case of no errors the return is an emtpy list.
    """

    print(f"Start processing for the year: {year_process} and the month: {month}")
    spread_2023 = os.environ["MASTER_SPREADSHEET_ID"]
    spread_2024 = os.environ["MASTER_SPREADSHEET_ID_24"]
    spread_2025 = os.environ["MASTER_SPREADSHEET_ID_25"]

    master_selector = {"2023": spread_2023, "2024": spread_2024, "2025": spread_2025}
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
            print(f"Error in process_sheets_from_master {e} ---- {type(e)}.")

    try:
        results_df = get_dataframe(month=month, year=year_process)
    except Exception as e:
        print(
            f"Error in process_sheets_from_master writting failed to bucket, message: {e} and type: {type(e)}"
        )
        write_failed_sheets_to_json("All", year_process, month, attempt=attempts)
        return []
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
    names_not_updated = updater(results_df, hostess_dict, year_process, month)

    if (
        isinstance(names_not_updated, list)
        and len(names_not_updated) > 0
        and attempts > 1
    ):
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
