import os
import pandas as pd
import gspread
import google.auth
from datetime import datetime

def update_extra(sheet_name):
    EXTRA_SHEET_ID = os.environ["EXTRA_SHEET_ID"]
    EXTRA_H = os.environ["EXTRA_H"]
    DATASET = os.environ["DATASET"]
    PROJECT_ID = os.environ["PROJECT_ID"]

    if sheet_name == "MASTER":
      return

    credentials, _ = google.auth.default()
    gc = gspread.authorize(credentials)
    integer_cols = {"tip_ret":"int64","x_20":"int64","x_395":"int64","okuri":"int64","disc":"int64","adv":"int64"}
    try:
        extra_sh = gc.open_by_key(EXTRA_SHEET_ID)
        ws = extra_sh.worksheet(sheet_name)

        df =pd.DataFrame(ws.get_all_values())
        df = df.iloc[1:]
        df = df[df.columns[0:10]]
        df.columns = ["DAY","NAME","CODE","tip_ret","x_20","x_395","okuri","disc","adv","notes"]

        # return df
        ### Clean the data
        for col in integer_cols.keys():
            df[col] = df[col].map(lambda x: x.replace(",","").replace(" ","").replace("(","-").replace(")","") if isinstance(x,str) else x)
            df[col] = df[col].map(lambda x: 0 if isinstance(x,str) and x == "" else x)
        df = df.astype({f'{col}':f'{integer_cols.get(col,"string")}' for col in df.columns})
        for col in df.columns:
            if col not in integer_cols.keys():
                df[col] = df[col].map(lambda x: None if isinstance(x,str) and x == "" else x)

        df = df[df['DAY'].notna()]
        df["UPLOADED"] = datetime.now()
        df.to_gbq(
        destination_table=f"{DATASET}.{EXTRA_H}",
        project_id=PROJECT_ID,
        progress_bar=False,
        if_exists="replace")

        return True
    except Exception as e:
        print(e)
        return False
