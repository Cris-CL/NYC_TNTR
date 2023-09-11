import os
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import gspread
from time import sleep

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET = os.environ["DATASET"]
TABLE_1 = os.environ["TABLE_1"]
TABLE_2 = os.environ["TABLE_2"]
spreadsheet_name = os.environ["SPREADSHEET_NAME"]

QUERY_ASSIS = f"""
WITH wages as (with assist as (SELECT DISTINCT
  DAY,
  store_number,
  store_name,
  assis.hostess_name,
  real_name,
  job_category,
  assis.cp_code,
  payroll_system,
  guaranteed_hourly_rate,
  slide_hourly_rate,
  earnings_slide,
  average_hourly_wage,
  working_days,
  working_hours,
  start_time,
  leave_time,
  absence,
  absence_amount,
  late_comming,
  late_amount,
  earnings,
  earnings_subtotal,
  earnings_excluding_tax,
  PARSE_TIME('%H%M', start_time) AS start_time_p,
  PARSE_TIME('%H%M', leave_time) AS leave_time_p,

  CASE
    WHEN start_time <= leave_time THEN
      ROUND(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%H%M', leave_time),
        PARSE_TIMESTAMP('%H%M', start_time),
        HOUR
      ) +
      MOD(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%H%M', leave_time),
        PARSE_TIMESTAMP('%H%M', start_time),
        MINUTE), 60) / 60.0,2)
    ELSE
      ROUND(24 + TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%H%M', leave_time),
        PARSE_TIMESTAMP('%H%M', start_time),
        HOUR
      ) +
      MOD(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%H%M', leave_time),
        PARSE_TIMESTAMP('%H%M', start_time),
        MINUTE), 60) / 60.0,2)
  END AS work_time_calculated,


CASE
  WHEN
    PARSE_TIMESTAMP('%H%M', start_time) between  PARSE_TIMESTAMP('%H%M', "0000") AND PARSE_TIMESTAMP('%H%M', "0800")  then PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY as INT64)+1," ",start_time))
    ELSE
      PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY as INT64)," ",start_time))
END AS start_datetime,
CASE
  WHEN
    PARSE_TIMESTAMP('%H%M', leave_time) between  PARSE_TIMESTAMP('%H%M', "0000") AND PARSE_TIMESTAMP('%H%M', "0800")  then PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY as INT64)+1," ",leave_time))
    ELSE
      PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY as INT64)," ",leave_time))
END AS leave_datetime,
  wa.hour_a,
  wa.hour_b,



FROM `{PROJECT_ID}.{DATASET}.{TABLE_1}` as assis
LEFT JOIN `{PROJECT_ID}.{DATASET}.{TABLE_2}` as wa
on wa.hostess_name = assis.hostess_name

order by CAST(DAY as INT64) ASC

)
SELECT
*,
CASE
    WHEN leave_datetime <= CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME)
      THEN ROUND(datetime_diff(leave_datetime, start_datetime, MINUTE) / 15,0) * 15 / 60
    WHEN start_datetime < CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME)
      AND leave_datetime >= CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME)
      THEN
        ROUND(datetime_diff(CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME),start_datetime, MINUTE)/60,2)
    ELSE 0
  END as a_wage,

  CASE
  WHEN leave_datetime >= CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME)
    THEN
      ROUND(datetime_diff(leave_datetime,CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME), MINUTE)/60,2)
  ELSE 0
  END as b_wage
from assist
order by CAST(DAY as INT64) ASC
)
SELECT
*,
hour_a*a_wage + hour_b*b_wage as wage_total_daily

from wages
order by CAST(DAY as INT64) ASC, cp_code asc

"""

# Initialize Google Sheets and BigQuery clients
gc = gspread.service_account()

# BigQuery query
query = QUERY_ASSIS
bq_client = bigquery.Client.from_service_account_json()  ## TODO: add the json file path here
query_job = bq_client.query(query)
results_df = query_job.to_dataframe()

# Convert time columns to string

for col in results_df.columns:
    results_df[col] = results_df[col].astype(str)

# Specify the Google Sheets document and worksheet


# Open the Google Sheets document
sh = gc.open(spreadsheet_name)


def format_worksheet(worksheet):
    """Format the worksheet to adjust column width and format the cells"""
    format_options = {
        'A': {'numberFormat': {'type': 'NUMBER', 'pattern': '0'}},  # Number, no decimals
        'B': {'numberFormat': {'type': 'NUMBER', 'pattern': '0'}},  # Number, no decimals
        'C': {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}},  # Number, 2 decimals
        'F': {'numberFormat': {'type': 'TIME', 'pattern':"hh:mm:ss am/pm"}},  # Time format
        'G': {'numberFormat': {'type': 'TIME', 'pattern':"hh:mm:ss am/pm"}},  # Time format
        'H': {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}},  # Number, 2 decimals
        'I': {'numberFormat': {'type': 'CURRENCY', 'pattern': "[$¥-411]#,##0"}}  # Number, no decimals
    }
    ## Using batch update to format the columns
    batch = [{"range":f"{col}2:{col}","format":options} for col, options in format_options.items()]
    worksheet.batch_format(batch)
    return


def update_google_sheets_with_retry(results_df):

    def clean_cell(unit_cell):
        if unit_cell.value.lower() in ['nan','none','nat','null',' ']:
                unit_cell.value = ''
        else:
            try:
                unit_cell.value = float(unit_cell.value)
            except:
                pass
        return unit_cell

    columns_ind = ['DAY',
                   'working_days',
                   'working_hours',
                   'start_time',
                   'leave_time',
                   'start_time_p',
                   'leave_time_p',
                   'work_time_calculated',
                   'wage_total_daily']

    for name in results_df["hostess_name"].unique():
        while True:
            try:
                sheets = [sht_name.title for sht_name in sh.worksheets()]
                df_temp = results_df[results_df["hostess_name"]==name][columns_ind].copy()

                if name not in sheets:
                    new_sheet_name = name
                    new_worksheet =  sh.add_worksheet(title=name,
                                                      rows=str(df_temp.shape[0]),
                                                      cols=str(df_temp.shape[1]))
                else:
                    new_worksheet = sh.worksheet(name)

                new_worksheet.clear()
                cell_list = new_worksheet.range(1, 1, len(df_temp)+1, len(df_temp.columns))

                for cell in cell_list:
                    if cell.row == 1:
                        # Set headers in the first row
                        cell.value = df_temp.columns[cell.col-1]
                    else:
                        # Set data from the DataFrame without single quotes
                        cell.value = str(df_temp.iloc[cell.row-2, cell.col-1])

                cell_list = [clean_cell(cell_dirty) for cell_dirty in cell_list]

                new_worksheet.update_cells(cell_list)
                try:
                    format_worksheet(new_worksheet)
                except:
                    print(f"Couldn't format {name} Sheet")
                break  # Exit the retry loop if successful
            except Exception as e:
                if "RATE_LIMIT_EXCEEDED" in str(e):
                    print("API rate limit exceeded. Waiting and retrying...")
                    sleep(60)  # Wait for 60 seconds before retrying
                    print(f"retrying {name} sheet")
                else:
                    raise  # Re-raise the exception if it's not a rate limit error
