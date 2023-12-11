import os
import pandas as pd
import gspread
import google.auth
from datetime import datetime
from google.cloud import bigquery


EXTRA_SHEET_ID = os.environ["EXTRA_SHEET_ID"]
EXTRA_H = os.environ["EXTRA_H"]
DATASET = os.environ["DATASET"]
PROJECT_ID = os.environ["PROJECT_ID"]
WAGES_SHEET_ID = os.environ["WAGES_SHEET_ID"]


def replace_extra():

    client = bigquery.Client()
    query = f"""
    DELETE
    `{DATASET}.{EXTRA_H}`
    WHERE  UPLOADED < (
    SELECT MAX(UPLOADED)
    FROM `{DATASET}.{EXTRA_H}`)
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        print("Cleaned extra dataset")
    except Exception as e:
        print(f'replace_extra failed with error: {e}')
        raise e


    return

def get_hostess_code(master_id):
    try:
        credentials, _ = google.auth.default()
        gc = gspread.authorize(credentials)

        nyc_master_hostess_data = gc.open_by_key(master_id)
        worksheet_name = "MASTER"  #### Maybe this should be an environment variable
        worksheet = nyc_master_hostess_data.worksheet(worksheet_name)

        a_col = worksheet.get_values("A2:A")
        d_col = worksheet.get_values("D2:D")

        hostes_dict = {A[0]:D[0] for A,D in zip(a_col,d_col)}
    except Exception as e:
        print(e)
        print(f"get_hostess_code failed with error: {e}")
        return False
    return hostes_dict

def replace_loading_codes(df, code_dict):
    # Iterate through the DataFrame rows
    for index, row in df.iterrows():
        name = row['NAME']
        code = row['CODE']

        if "oading" in code:
            # Replace "Loading..." with the correct code from the dictionary
            if name in code_dict:
                df.at[index, 'CODE'] = code_dict[name]

    return df


def update_extra(sheet_name):


    if sheet_name == "MASTER":
      return
    replace_extra()
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
        try:
          name_code = get_hostess_code(WAGES_SHEET_ID)
          df = replace_loading_codes(df,name_code)
        except Exception as e:
          print("Couldnt remove the loading values")
          print(e)

        df.to_gbq(
        destination_table=f"{DATASET}.{EXTRA_H}",
        project_id=PROJECT_ID,
        progress_bar=False,
        if_exists="replace")
        replace_extra()

        return True
    except Exception as e:
        print(e)
        return False
