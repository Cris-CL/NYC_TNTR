import os
import pandas as pd
import gspread
import google.auth
from datetime import datetime

def update_online_sales(sheet_name):

    OSALES_SHEET_ID = os.environ["OSALES_SHEET_ID"]
    ONLINE_TABLE = os.environ["ONLINE_TABLE"]
    DATASET = os.environ["DATASET"]
    PROJECT_ID = os.environ["PROJECT_ID"]

    credentials, _ = google.auth.default()
    gc = gspread.authorize(credentials)

    online_data = gc.open_by_key(OSALES_SHEET_ID)

    od_sales = online_data.worksheet(sheet_name)

    df =pd.DataFrame(od_sales.get_all_values())
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df = df[df.columns[0:15]]
    df.columns = [col.replace(" ","_").replace("(","").replace(")","").replace("/","_").lower() for col in df.columns]
    df = df.astype({"created":"datetime64[ns]",
                    "amount":"int64",
                    "fees":"int64",
                    "net":"int64"})

    df = df[df['transfer'].notna()]
    df["UPLOADED"] = datetime.now()
    df.to_gbq(
      destination_table=f"{DATASET}.{ONLINE_TABLE}",
      project_id=PROJECT_ID,
      progress_bar=False,
      if_exists="replace")

    return df.copy()
