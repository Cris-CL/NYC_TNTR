import gspread
import google.auth
from time import sleep
import calendar
import re
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
    # Search for "Please try again in XX seconds" pattern
    retry_match = re.search(r"please try again in (\d+) seconds", error_message)
    print(retry_match)
    if retry_match:
        print(f"API error in {function_name}")
        # Extract the number of seconds to sleep
        sleep_time = int(retry_match.group(1)) + 1
        print(f"Sleeping for {sleep_time} seconds.")
        sleep(sleep_time)

        # Save the error message to a text file in the bucket
        current_date = datetime.now().strftime("%y%m%d")
        file_name = f"{current_date}_apierror_message.txt"
        # save_error_to_bucket(error_message, file_name, bucket_name)
        print(f"Retrying {function_name}")
        return True
    else:
        return False


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
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Create a new blob and upload the message
    blob = bucket.blob(file_name)
    blob.upload_from_string(message)
    return


def get_hostess_dict(master_id):
    try:
        gc = gspread.service_account()
    except:
        credentials, _ = google.auth.default()
        gc = gspread.authorize(credentials)
    try_number = 0
    while True:
        try:
            nyc_master_hostess_data = gc.open_by_key(master_id)
            worksheet_name = "MASTER"  #### Maybe this should be an environment variable
            worksheet = nyc_master_hostess_data.worksheet(worksheet_name)

            a_col = worksheet.get_values("A2:A")
            p_col = worksheet.get_values("P2:P")

            hss_dict = {A[0]: P[0] for A, P in zip(a_col, p_col)}
            break
        except Exception as e:
            handler = handle_gspread_error(e, "get_hostess_dict", "nonoe")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
            else:
                print("Error in get_hostess_dict", e, type(e))
                return {}
    return hss_dict


def clear_formatting(FILE, sheet_name):
    try_number = 0
    while True:
        try:
            wsht = FILE.worksheet(sheet_name)
            sheetId = int(wsht._properties["sheetId"])
            body = {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {"sheetId": sheetId},
                            "cell": {"userEnteredFormat": {}},
                            "fields": "userEnteredFormat",
                        }
                    }
                ]
            }
            break
        except Exception as e:
            handler = handle_gspread_error(e, "clear_formatting part_1", "nonoe")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
            else:
                raise e
    try_number = 0
    while True:
        try:
            FILE.batch_update(body)
            break
        except Exception as e:
            handler = handle_gspread_error(e, "clear_formatting part_2", "nonoe")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
            else:
                print(
                    f"Error in clear_formatting on file: {FILE.title} and sheet: {sheet_name}",
                    e,
                    type(e),
                )
    return


def format_worksheet(worksheet):
    format_waiting = 5

    cell_types = {
        "number": {"numberFormat": {"type": "NUMBER", "pattern": "0"}},
        "number_decimal": {"numberFormat": {"type": "NUMBER", "pattern": "#0.#0"}},
        "time": {"numberFormat": {"type": "TIME", "pattern": "hh:mm:ss am/pm"}},
        "currency": {"numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}},
    }

    col_types = {
        "C": "time",
        "D": "time",
        "E": "number_decimal",
        "F": "currency",
        "G": "currency",
        "H": "currency",
        "I": "currency",
        "J": "currency",
        "K": "currency",
        "L": "currency",
        "M": "currency",
        "N": "currency",
        "O": "currency",
        "P": "currency",
        "Q": "currency",
        "R": "currency",
        "S": "currency",
        "T": "currency",
        "U": "currency",
        "V": "currency",
        "W": "currency",
        "X": "currency",
        "Y": "currency",
        "Z": "currency",
        "AA": "currency",
        "AB": "currency",
        "AC": "currency",
        "AD": "number",
    }
    num_rows = len(worksheet.get_all_values())
    format_options = {
        col: cell_types.get(col_types.get(col, "number"), cell_types["number"])
        for col in col_types.keys()
    }
    batch = [
        {
            "range": f"{col}2:{col}",
            "format": options,
            "valueInputOption": "USER_ENTERED",
        }
        for col, options in format_options.items()
    ]
    batch_2 = [
        {
            "range": f"B{num_rows - 4}",
            "format": cell_types["currency"],
            "valueInputOption": "USER_ENTERED",
        },
        {
            "range": f"B{num_rows - 3}",
            "format": cell_types["currency"],
            "valueInputOption": "USER_ENTERED",
        },
        {
            "range": f"B{num_rows - 2}",
            "format": cell_types["currency"],
            "valueInputOption": "USER_ENTERED",
        },
        {
            "range": f"B{num_rows - 1}",
            "format": cell_types["currency"],
            "valueInputOption": "USER_ENTERED",
        },
        {
            "range": f"B{num_rows}",
            "format": cell_types["currency"],
            "valueInputOption": "USER_ENTERED",
        },
    ]
    batch = batch + batch_2
    try_number = 0
    while True:
        try:
            worksheet.batch_format(batch)
            break
        except Exception as e:
            handler = handle_gspread_error(e, "format_worksheet", "none")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
            elif "exhausted" in str(e).lower() or "exeeded" in str(e).lower():
                print(
                    f"Waiting for {format_waiting} seconds before retrying format_worksheet"
                )
                sleep(format_waiting)
                format_waiting = format_waiting + 1
            else:
                print(f"Error in format_worksheet for {worksheet.title}", e, type(e))
                raise e
    return


def resize_columns(FILE, sheet_name):
    try_number = 0
    while True:
        try:
            wsht = FILE.worksheet(sheet_name)
            break
        except Exception as e:
            handler = handle_gspread_error(e, "resize_columns part_1", "none")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
    sheetId = int(wsht._properties["sheetId"])
    body = {
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheetId,
                        "dimension": "COLUMNS",
                        "startIndex": 1,
                        "endIndex": 30,
                    }
                }
            }
        ]
    }
    try_number = 0
    while True:
        try:
            FILE.batch_update(body)
            return
        except Exception as e:
            handler = handle_gspread_error(e, "resize_columns part_2", "none")
            if handler == True and try_number == 0:
                try_number = try_number + 1
                continue
            elif "exhausted" in str(e).lower() or "exceeded" in str(e).lower():
                print(
                    f"API rate limit exceeded. Waiting 5 and retrying formatting {sheet_name} sheet"
                )
                sleep(5)
                continue
            else:
                file_name = int(wsht._properties)
                print(
                    f"resize_columns: Couldnt resize the sheet {sheet_name} on {file_name}",
                    e,
                    type(e),
                )
                return


def clean_cell(unit_cell):
    if unit_cell.value.lower() in ["nan", "none", "nat", "null", "<na>", " "]:
        unit_cell.value = ""
    else:
        pass
    try:
        unit_cell.value = float(unit_cell.value)
    except:
        pass
    return unit_cell


def days_in_month(date_string):
    try:
        year = int(date_string[0:4])
        month = int(date_string[4:6])
    except ValueError:
        return "Invalid date format. Please use 'yyyymmdd' format."

    _, num_days = calendar.monthrange(year, month)
    return num_days


def calc_gensen(subtotal, days_in_month):
    """
    Calculates the 源泉徴収税 (gensen) for the current hostess, the formula is the subtotal minus 5,000 yen times the
    amount of days in the current month, if that value is more than 0 then multiply it by 0.1021, round it and make
    it negative, in case the value is less than 0 then returns 0.

    Args:
        subtotal (int): The wage plus commission earned in the month.
        days_in_month (int): The amount of days the current month has.

    Returns:
        int: The negative and  rounded gensen.
    """
    gensen = (subtotal - 5000 * days_in_month) * 0.1021
    if gensen > 0:
        return round(-gensen)
    else:
        return 0
