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


def get_spreadsheet(spreadsheet_name=None):
    # Specify the Google Sheets document and worksheet
    credentials, _ = google.auth.default()
    gc = gspread.authorize(credentials)
    # Open the Google Sheets document
    sh = gc.open(spreadsheet_name)
    return sh


def get_dataframe(month=8,year=2023):
    QUERY_ASSIS = create_query(month,year=2023)
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


def format_worksheet(worksheet):
    # Define the formatting options for each column
    format_options = {
        "A": {
            "numberFormat": {"type": "NUMBER", "pattern": "0"}
        },  # Number, no decimals
        "B": {
            "numberFormat": {"type": "NUMBER", "pattern": "0"}
        },  # Number, no decimals
        "C": {
            "numberFormat": {"type": "TIME", "pattern": "hh:mm:ss am/pm"}
        },  # Time format
        "D": {
            "numberFormat": {"type": "TIME", "pattern": "hh:mm:ss am/pm"}
        },  # Time format
        "E": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "F": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "G": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "H": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "I": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "J": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "K": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "L": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "M": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "N": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "O": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "P": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "Q": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
        "R": {
            "numberFormat": {"type": "CURRENCY", "pattern": "[$¥-411]#,##0"}
        },  # Number, no decimals
    }
    batch = [
        {"range": f"{col}2:{col}", "format": options}
        for col, options in format_options.items()
    ]
    while True:
        try:
            worksheet.batch_format(batch)
            break
        except Exception as e:
            if "exhausted" in str(e).lower():
                format_waiting = 5
                print(f"Waiting for {format_waiting} seconds before retrying")
                sleep(format_waiting)
                format_waiting = format_waiting * 1.2
            else:
                print("problem with formatting")
                raise

    return


def update_google_sheets_with_retry(results_df, sh, hostess_name):
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

    columns_ind = [
        "DAY",
        "cp_code",
        "start_time_p",
        "leave_time_p",
        "wage_total_daily",
        "commision_DR1",
        "commision_DR2",
        "commision_DR3",
        "commision_BT",
        "commision_FD",
        "commision_KA",
        "commision_OT",
        "commision_TB",
        "commision_TP",
        "commision_EN",
        "commision_EX",
        "commision_CR",
        "commision_DH",
        "commision_HC",
    ]
    ## In case the hostess_name is "all" we will process all the hostess
    ## In case the hostess_name is not "all" we will process only that hostess

    if hostess_name == "all":
        list_hostess = results_df["hostess_name"].unique()
    elif hostess_name != "all":
        list_hostess = [hostess_name]
    ## TODO : add the month number to the query
    for name in list_hostess:
        while True:
            try:
                df_temp = results_df[results_df["hostess_name"] == name][
                    columns_ind
                ].copy()
                df_temp.columns = [
                    column_before.replace("commision_", "")
                    for column_before in df_temp.columns
                ]
                sheets = [sht_name.title for sht_name in sh.worksheets()]

                if name not in sheets:
                    new_sheet_name = name
                    new_worksheet = sh.add_worksheet(
                        title=name,
                        rows=str(df_temp.shape[0]),
                        cols=str(df_temp.shape[1]),
                    )
                else:
                    new_worksheet = sh.worksheet(name)

                new_worksheet.clear()
                cell_list = new_worksheet.range(
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

                new_worksheet.update_cells(cell_list, value_input_option="USER_ENTERED")
                try:
                    format_worksheet(new_worksheet)
                except Exception as e:
                    print(f"Couldn't format {name} Sheet")
                    print(e)
                break  # Exit the retry loop if successful
            except Exception as e:
                if "RATE_LIMIT_EXCEEDED" in str(e):
                    waiting_time = 10
                    print(
                        f"API rate limit exceeded. Waiting {waiting_time} and retrying {name} sheet"
                    )
                    sleep(waiting_time)  # Wait for 10 seconds before retrying, then wait 20, then 30, etc.
                    waiting_time = waiting_time + 10
                else:
                    print("Some other error ocurred")
                    print(e)
                    raise  # Re-raise the exception if it's not a rate limit error


def main_process(name, month):
    spreadsheet_name = os.environ["SPREADSHEET_NAME"]
    if name == "test":
        print("test_run")
        return
    results_df = get_dataframe(month=month)
    sh = get_spreadsheet(spreadsheet_name)
    update_google_sheets_with_retry(results_df, sh, name)
    print(f"Finished processing {name} for the month {month}")
