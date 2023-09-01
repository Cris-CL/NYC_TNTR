import pytz
import pandas as pd
from datetime import datetime

def get_timestamp(timezone_name):
  dt = datetime.now(pytz.timezone(timezone_name))
  timestamp = dt.strftime(("%Y-%m-%d %H:%M:%S"))
  return timestamp

def get_date_from_file(file_name):
    print(file_name)
    date_ls = file_name.replace("Assis","").replace(".xlsx","").replace(" ","").split("-")
    print(date_ls)
    timezone_name = "Asia/Tokyo"
    stamp_now = datetime.now(tz=pytz.timezone(timezone_name))
    ## day format
    if len(date_ls[0]) == 1:
        date_ls[0] = f"0{date_ls[0]}"
    elif len(date_ls[0]) >2:
        return print("incorrect day format")
    ## month format
    if len(date_ls[1]) == 1:
        date_ls[1] = f"0{date_ls[1]}"
    elif len(date_ls[1]) >2:
        return print("incorrect month format")
    print(date_ls)
    date_2 = f'{stamp_now.year}{date_ls[0]}{date_ls[1]}'
    return date_2

def add_file_name_to_df(df,file_name):
    df["FILE_NAME"] = file_name
    return df

def add_date_to_df(df):
    date = get_timestamp("Asia/Tokyo")
    df["DATE"] = date
    return df

def upload_bq(df,table_id,project_id):
    df.to_gbq(
            destination_table=table_id,
            project_id=project_id,
            progress_bar=False,
            if_exists="replace")
    return

def identify_file(file_name):
    if "assis" in file_name.lower():
        return "assis"
    elif "nippo" in file_name.lower():
        return "nippo"
    elif "goukei sh" in file_name.lower():
        return "shosai"
    elif "goukei d" in file_name.lower():
        return "goukei data"
    else:
        return "unknown"

def load_file(file_name):
    file_type = identify_file(file_name)
    if file_type == "assis":
        df = pd.read_excel(file_name,index_col=False,skipfooter=1)
        df = add_file_name_to_df(df,file_name)
        df = add_date_to_df(df)
        df["DAY"] = get_date_from_file(file_name)
        return df
    elif file_type == "nippo":
        df = pd.read_excel(file_name,index_col=False,skipfooter=2)
        df = add_file_name_to_df(df,file_name)
        df = add_date_to_df(df)
        return df
    elif file_type == "shosai":
        df = pd.read_excel(file_name)
        df = add_file_name_to_df(df,file_name)
        df = add_date_to_df(df)
        return df
    elif file_type == "goukei data":
        df = pd.read_excel(file_name,index_col=False,skiprows=5)
        df = add_file_name_to_df(df,file_name)
        df = add_date_to_df(df)
        return df
    else:
        return print("unknown file type")
