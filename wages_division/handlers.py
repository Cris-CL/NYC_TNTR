from time import sleep
import re
import os
from datetime import datetime
from google.cloud import storage


def handle_gspread_error(error, function_name, bucket_name):
    """
    Handles gspread html errors that occur during processing.

    Args:
        error (Exception): The exception that occurred.
        function_name (str): The name of the function where the error happened.
    """
    error_message = str(error).lower()
    retry_match = re.search(r"please try again in (\d+) seconds", error_message)
    current_date = datetime.now().strftime("%y%m%d")
    file_name = f"{current_date}_{function_name}_handler.txt"

    if retry_match:
        print(f"handle_gspread_error: match= {retry_match}")
        print(f"API error in {function_name}")
        # Extract the number of seconds to sleep
        sleep_time = int(retry_match.group(1)) + 1
        print(f"Sleeping for {sleep_time} seconds.")
        sleep(sleep_time)

        # Save the error message to a text file in the bucket
        print(f"Retrying {function_name}")
        function_return = True
    else:
        print(f"handle_gspread_error other error in {function_name}")
        if len(error_message) < 300:
            print(error_message)
        function_return = False

    save_error_to_bucket(error_message, file_name, bucket_name)
    return function_return


def save_error_to_bucket(message, file_name, bucket_name):
    """
    Creates a file with the contents of special errors and places it on
    a bucket to not print huge statements in the logs.

    Args:
        message (str): The contents of the error message.
        file_name (str): The name of the file to be created.
        bucket_name (str): The name of the bucket where the file will be placed.
    """
    # Initialize the Cloud Storage client
    ERROR_BUCKET = os.environ["ERROR_BUCKET"]
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(ERROR_BUCKET)
        message = str(message)
        # Create a new blob and upload the message
        blob = bucket.blob(file_name)
        print(f"Saving error message to bucket {ERROR_BUCKET}/{file_name}")
        blob.upload_from_string(message)
    except Exception as e:
        print(
            f"Error in save_error_to_bucket message: {e}, couldn't save error info to bucket"
        )

    return


def handle_rate_limit(waiting_time, name):
    """
    Handles API rate limit errors by waiting and retrying.

    Args:
        waiting_time (int): The current waiting time in seconds.
        name (str): The name of the hostess being processed.

    Returns:
        int: The updated waiting time for the next retry.
    """
    print(
        f"API rate limit exceeded. Waiting {waiting_time} seconds and retrying {name} sheet"
    )
    sleep(waiting_time)
    return waiting_time + 5


def handle_other_errors(name, error):
    """
    Handles other errors that occur during processing.

    Args:
        name (str): The name of the hostess being processed.
        error (Exception): The exception that occurred.
    """
    err_txt = str(error).lower()
    if "http" in err_txt:
        print(
            f"Other error handler -- got http error while processing {name} type {type(error)}"
        )
    else:
        print(
            f"Other error handler -- An error occurred while processing {name} message: {error} type: {type(error)}"
        )
    return
