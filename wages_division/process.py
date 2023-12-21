# pandas==1.5.1
# google-cloud-bigquery>=3.3.5
# gspread==5.7.2

import os
import pandas as pd
import google.auth
from google.cloud import bigquery
import gspread
from time import sleep
from query import create_query
from new_query import create_new_query
from get_spread_info import get_hostess_dict
import locale
import calendar
import datetime
import json
import requests
from google.cloud import storage


def write_failed_sheets_to_json(bucket_name, file_name, names, year, month):
    failed_sheets = {
        "type": "retry",
        "attempt": 1,
        "year": year,
        "month": month,
        "names": names,
    }
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


def reverse_list_odd_date(lst):
    current_date = datetime.datetime.now().day

    if current_date % 2 == 0:
        return lst
    else:
        reversed_list = list(reversed(lst))
        return reversed_list


def get_spreadsheet(spreadsheet_name=None, spreadsheet_id=None):
    # Specify the Google Sheets document and worksheet

    credentials, _ = google.auth.default()
    gc = gspread.authorize(credentials)

    # Open the Google Sheets document
    if spreadsheet_id:
        sh = gc.open_by_key(spreadsheet_id)
    else:
        sh = gc.open(spreadsheet_name)
    return sh


def get_dataframe(month=9, year=2023):
    QUERY_ASSIS = create_new_query(month, year=2023)
    # Initialize Google Sheets and BigQuery clients

    # BigQuery query
    query = QUERY_ASSIS
    bq_client = bigquery.Client()
    query_job = bq_client.query(query)
    results_df = query_job.to_dataframe()

    # Convert time columns to string

    for col in results_df.columns:
        results_df[col] = results_df[col].astype(str)
    return results_df


def get_specific_hostess_df(df, hostess_name):
    loc_df = df.copy()
    columns_ind = [
        "DAY",
        "cp_code",
        "starting_time",
        "leaving_time",
        "worked_hours",
        "wage_total_daily",
        "BT",
        "DR1",
        "DR2",
        "DR3",
        "TP",
        "DH",
        "HC",
        "MR",
        "HR",
        "X",
        "TOTAL_NOT_T",
        "BT_T",
        "DR1_T",
        "DR2_T",
        "DR3_T",
        "TP_T",
        "KA_T",
        "X395",
        "TOTAL_T",
        "TOTAL_DAY",
        "送り",
        "DISC",
        "ADV",
        "notes",
    ]
    new_names = {
        "wage_total_daily": "SUBTOTAL_WAGE",
        "TOTAL_NOT_T": "TOTAL_UC",
        "X": "EXTRA",
        "X395": "EXTRA_395",
        "TOTAL_T": "SUBTOTAL_395",
        # '':'',
    }

    loc_df = loc_df[loc_df["hostess_name"] == hostess_name][columns_ind].copy()

    loc_df.columns = [new_names.get(col_old, col_old) for col_old in loc_df.columns]

    loc_df.columns = [col.replace("_T", "_395") for col in loc_df.columns]

    return loc_df


def format_worksheet(worksheet):
    format_waiting = 5

    cell_types = {
        "number": {"numberFormat": {"type": "NUMBER", "pattern": "0"}},
        "number_decimal": {"numberFormat": {"type": "NUMBER", "pattern": "#.#0"}},
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
    while True:
        try:
            worksheet.batch_format(batch)
            break
        except Exception as e:
            if "exhausted" in str(e).lower() or "exeeded" in str(e).lower():
                print(
                    f"Waiting for {format_waiting} seconds before retrying format_worksheet"
                )
                print(e)
                sleep(format_waiting)
                format_waiting = format_waiting + 1
            else:
                print("problem with formatting")
                raise

    return


def resize_columns(FILE, sheet_name):
    wsht = FILE.worksheet(sheet_name)
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
    while True:
        try:
            FILE.batch_update(body)
            return
        except Exception as e:
            if "exhausted" in str(e).lower() or "exceeded" in str(e).lower():
                print(
                    f"API rate limit exceeded. Waiting 5 and retrying formatting {sheet_name} sheet"
                )
                sleep(5)
            else:
                print(e)
                print(f"Couldnt resize the sheet {sheet_name}")
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

    if (subtotal - 5000 * days_in_month) * 0.1021 > 0:
        return round(-(subtotal - 5000 * days_in_month) * 0.1021)
    else:
        return 0


def calc_totals_nrws(worksheet, year, month):
    date_str = f"{year}{month}"
    days_in_mon = days_in_month(date_str)
    try:
        # Get all values in the worksheet
        values = worksheet.get_all_values()

        # Add two empty rows at the bottom
        num_rows = len(values)
        num_columns = len(values[0])
        for _ in range(2):
            values.append([""] * num_columns)

        # Calculate and set totals in the third row
        start_column = 5  # Column E
        end_column = 29  # Column AC

        subtotal_row = [""] * num_columns
        discounts_row = [""] * num_columns
        grand_total_row = [""] * num_columns

        gensen_row = [""] * num_columns
        paid_amount_row = [""] * num_columns

        for col in range(start_column, end_column + 1):
            try:
                column_values = []
                for row in values:
                    cell_value = row[col - 1]
                    if cell_value.strip().replace(".", "", 1).isdigit():
                        column_values.append(float(cell_value))
                    else:
                        # Convert yen currency-formatted values to float
                        try:
                            currency_value = float(
                                cell_value.strip("¥").replace(",", "")
                            )
                            column_values.append(currency_value)
                        except ValueError:
                            pass

                column_sum = sum(column_values)
                subtotal_row[col - 1] = column_sum  # Adjust for 0-based index
            except ValueError:
                pass
        # return subtotal_row
        # Add "GRAND TOTAL" in column A
        def str_to_float(x):
            try:
                x = x.replace("¥", "").replace(",", "")
                return float(x)
            except:

                return 0

        # Calculate and add the sum of columns F and AB in column B
        column_F_values = [str_to_float(row[5]) for row in values]
        column_Z_values = [str_to_float(row[25]) for row in values]

        column_AA_values = [str_to_float(row[26]) for row in values]
        column_AB_values = [str_to_float(row[27]) for row in values]
        column_AC_values = [str_to_float(row[28]) for row in values]

        subtotal = sum(column_F_values) + sum(column_Z_values)

        subtotal_row[0] = "SUBTOTAL"
        subtotal_row[1] = subtotal

        discounts_row[0] = "OTHERS"
        discounts = (
            sum(column_AA_values) + sum(column_AB_values) + sum(column_AC_values)
        )
        discounts_row[1] = discounts

        gensen_row[0] = "源泉徴収税"
        gensen = calc_gensen(subtotal, days_in_mon)
        gensen_row[1] = gensen

        paid_amount_row[0] = "支払金額合計"
        paid_amount_row[1] = subtotal + gensen

        grand_total_row[0] = "GRAND TOTAL"
        grand_total_row[1] = subtotal + discounts + gensen

        # Append the totals row to the worksheet
        values.append(subtotal_row)
        values.append(gensen_row)
        values.append(paid_amount_row)
        values.append(discounts_row)
        values.append(grand_total_row)

        # Update the worksheet with the modified values
        worksheet.update(values, value_input_option="USER_ENTERED")

        return True
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False


def update_all_sheets(results_df, sh_hostess_dict, month):
    list_hostess = list(sh_hostess_dict.keys())
    list_hostess = reverse_list_odd_date(list_hostess)
    print(f"Processing all {len(list_hostess)} hostess")
    waiting_time = 10
    names_not_updated = []
    try:
        list_hostess.remove("店")
    except:
        print("店 not in the file")
    for name in list_hostess:
        while True:  ### This is to retry in case of API rate limit exceeded
            try:
                id_sh = sh_hostess_dict.get(name, None)
                if not id_sh:
                    print(f"Hostess {name} not found in the master spreadsheet")
                    break
                sh = get_spreadsheet(spreadsheet_id=id_sh)
                spreadsheet_name = sh.title
                if name.lower() not in spreadsheet_name.lower():
                    print(
                        f"Spreadsheet: {spreadsheet_name} doesnt match with hostess name: {name}"
                    )
                    break
                df_temp = get_specific_hostess_df(results_df, name)
                if df_temp.shape[0] == 0:
                    print(f"No data for {name}")
                    break

                sheets = [sht_name.title for sht_name in sh.worksheets()]

                year = 2023  ## TODO change this to a environment variable or another way to get the year
                month_str = (2 - len(str(month))) * "0" + str(
                    month
                )  ## Add a zero if the month is less than 10

                new_sheet_name = f"{year}{month_str}"

                if (
                    new_sheet_name not in sheets
                ):  ### HERE WE NEED TO CHECK IF THE MONTH IS ALREADY THERE
                    active_worksheet = sh.add_worksheet(
                        title=new_sheet_name,
                        rows=str(df_temp.shape[0]),
                        cols=str(df_temp.shape[1]),
                    )
                else:
                    active_worksheet = sh.worksheet(new_sheet_name)

                #### Here maybe we need to check if the day we want to update is already there
                active_worksheet.clear()  ### Maybe no need to clear the sheet, just append the new data

                cell_list = active_worksheet.range(
                    1, 1, len(df_temp) + 1, len(df_temp.columns)
                )

                for cell in cell_list:
                    if cell.row == 1:
                        # Set headers in the first row
                        cell.value = df_temp.columns[cell.col - 1]
                    else:
                        # Set data from the DataFrame without single quotes
                        cell.value = str(df_temp.iloc[cell.row - 2, cell.col - 1])
                cell_list = [clean_cell(cell_dirty) for cell_dirty in cell_list]

                active_worksheet.resize(
                    rows=str(df_temp.shape[0]), cols=str(df_temp.shape[1])
                )  ### Resize the sheet to the new data
                active_worksheet.update_cells(
                    cell_list, value_input_option="USER_ENTERED"
                )
                calc_totals_nrws(active_worksheet, year, month_str)
                try:
                    format_worksheet(active_worksheet)
                    resize_columns(FILE=sh, sheet_name=new_sheet_name)
                except Exception as e:
                    print(f"Couldn't format {name} Sheet")
                    print(e)
                # return print("finished test run")
                sleep(6)
                break  # Exit the retry loop if successful
            except Exception as e:
                if "RATE_LIMIT_EXCEEDED" in str(e):
                    print(
                        f"API rate limit exceeded. Waiting {waiting_time} and retrying {name} sheet"
                    )
                    sleep(
                        waiting_time
                    )  # Wait for 10 seconds before retrying, then wait 20, then 30, etc.
                    waiting_time = waiting_time + 5
                else:
                    print(f"Some other error ocurred while processing {name}")
                    names_not_updated.append(name)
                    print(e)
                    break
                    # raise  # Re-raise the exception if it's not a rate limit error
    try:
        if len(names_not_updated) > 0:
            print(
                f'Couldnt update the following people: {" ,".join(names_not_updated)}'
            )
            return names_not_updated
    except Exception as e:
        print(e, type(e))


def process_sheets_from_master(month, year_process, host_names="All"):
    MASTER_SPREADSHEET_ID = os.environ["MASTER_SPREADSHEET_ID"]

    hostess_dict = get_hostess_dict(MASTER_SPREADSHEET_ID)
    if host_names != "All":
        try:
            lis_names = (
                host_names.replace("[", "")
                .replace("]", "")
                .replace("'", "")
                .replace(" ", "")
                .split(",")
            )
            hostess_dict = {
                name_sing: hostess_dict.get(name_sing, "") for name_sing in lis_names
            }
        except:
            print("Couldn't process individual names")

    results_df = get_dataframe(month=month, year=year_process)
    try:
        names_in_df = results_df["hostess_name"].unique().tolist()
        hostess_dict = {
            k: hostess_dict[k] for k in names_in_df if k in hostess_dict.keys()
        }
    except Exception as e:
        print(f"Error while getting current df names: {e}")
        hostess_dict = get_hostess_dict(MASTER_SPREADSHEET_ID)

    if len(results_df) < 1:
        return print(f"No new data for the current month: {month} ")
    names_not_updated = update_all_sheets(results_df, hostess_dict, month)
    if isinstance(names_not_updated, list):
        print("Some updates failed, sending retry notice")
        BUCKET_RETRY = os.environ["BUCKET_RETRY"]
        today_date = datetime.date.today().strftime("%Y_%m_%d")
        file_name = f"failed_process_{today_date}_{year_process}{month}.json"
        write_failed_sheets_to_json(
            BUCKET_RETRY, file_name, names_not_updated, year_process, month
        )
    print(f"Finished processing all hostess for the month {month}")

    return
