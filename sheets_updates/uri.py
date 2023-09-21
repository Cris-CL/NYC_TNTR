import os
import google.auth
import gspread
from google.cloud import bigquery
from uri_query import get_uri_query


def update_uri_sheet(matching_column=1,start_date=0):
    # Authenticate with BigQuery and Google Sheets
    spreadsheet_name = os.environ.get("SPREADSHEET_NAME")
    worksheet_name = os.environ.get("WORKSHEET_NAME")

    bq_client = bigquery.Client()
    credentials, _ = google.auth.default()
    gc = gspread.authorize(credentials)

    # Open the Google Sheets spreadsheet

    sh = gc.open(spreadsheet_name) ### should be ID in the future to avoid problems when the doc changes name
    worksheet = sh.worksheet(worksheet_name)

# Get all existing values in the matching column of the worksheet
    existing_values = worksheet.col_values(matching_column)
    if len(existing_values) < 2:
        existing_values = [""]

    # Query BigQuery to fetch new data not in the worksheet
    query = get_uri_query(existing_values,start_date)
    query_job = bq_client.query(query)
    new_data = [list(row.values()) for row in query_job]

    # Append the new data to the worksheet

    if new_data:
        worksheet.append_rows(new_data,value_input_option="RAW")
        print(f"Appended {len(new_data)} new rows to {worksheet_name}.")
    else:
        print("No new data to append.")
