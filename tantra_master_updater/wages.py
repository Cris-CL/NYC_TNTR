import os
import pandas as pd
import gspread
import google.auth
from datetime import datetime
from google.cloud import bigquery

WAGES_SHEET_ID = os.environ.get("WAGES_SHEET_ID")
HOSTESS_WAGE = os.environ.get("HOSTESS_WAGE")
DATASET = os.environ.get("DATASET")
PROJECT_ID = os.environ.get("PROJECT_ID")


def replace_wages(year_month):

    client = bigquery.Client()
    query = f"""
        DELETE
        `{DATASET}.{HOSTESS_WAGE}`
        WHERE YEAR_MONTH = "{year_month}" and UPLOADED < (
        SELECT MAX(UPLOADED)
        FROM `{DATASET}.{HOSTESS_WAGE}`
        WHERE YEAR_MONTH = "{year_month}"
        )
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
    except Exception as e:
        raise e
    return


def get_wages(sheet_name):
    credentials, _ = google.auth.default()
    gc = gspread.authorize(credentials)
    integer_cols = {"hour_a": "int64", "hour_b": "int64", "cp_code": "int64"}
    try:
        wages_sh = gc.open_by_key(WAGES_SHEET_ID)
        ws = wages_sh.worksheet(sheet_name)

        df = pd.DataFrame(ws.get_all_values())
        df = df.iloc[1:]
        df = df[df.columns[0:7]]
        df.columns = [
            "hostess_name",
            "real_name",
            "wage_class",
            "cp_code",
            "hour_a",
            "hour_b",
            "YEAR_MONTH",
        ]
        ### Clean the data
        for col in integer_cols.keys():
            df[col] = df[col].map(
                lambda x: x.replace(",", "")
                .replace(" ", "")
                .replace("(", "-")
                .replace(")", "")
                .replace(".00", "")
                if isinstance(x, str)
                else x
            )
            df[col] = df[col].map(lambda x: 0 if isinstance(x, str) and x == "" else x)
        df = df.astype(
            {f"{col}": f'{integer_cols.get(col,"string")}' for col in df.columns}
        )
        for col in df.columns:
            if col not in integer_cols.keys():
                df[col] = df[col].map(
                    lambda x: None if isinstance(x, str) and x == "" else x
                )

        df["UPLOADED"] = datetime.now()
        df = df[df["YEAR_MONTH"].notna()]
    except Exception as e:
        print(e)
    return df


def update_wages(sheet_name):
    if sheet_name == "MASTER":
        return
    print(f"Sheet {HOSTESS_WAGE} with id: {WAGES_SHEET_ID} is being updated")

    try:
        df = get_wages(sheet_name)
        df.to_gbq(
            destination_table=f"{DATASET}.{HOSTESS_WAGE}",
            project_id=PROJECT_ID,
            progress_bar=False,
            if_exists="append",
        )

        ### Take year_month from current data and keep the latest uploaded version
        year_mon = df["YEAR_MONTH"].unique()[0]
        replace_wages(year_month=year_mon)
        print(f"Correctly updated {year_mon} sheet")

    except Exception as e:
        print(e)
        return False
