import pytz
import pandas as pd
from datetime import datetime
from assis import assis_jp_en
from goukei_data import gd_jp_en
from goukei_shosai import gs_jp_en
from nippo import nippo_jp_en

def get_timestamp(timezone_name):
  dt = datetime.now(pytz.timezone(timezone_name))
  timestamp = dt.strftime(("%Y-%m-%d %H:%M:%S"))
  return timestamp

def get_date_from_file(file_name):
    print(file_name)
    date_ls = file_name.replace("Assis","").replace(".xlsx","").replace(" ","").split("-")
    # print(date_ls)
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
    # print(date_ls)
    date_2 = f'{stamp_now.year}{date_ls[0]}{date_ls[1]}'
    return date_2

def get_month_nippo(file_name):
    month = file_name.split("-")[0].split(" ")[-1]
    if len(month) == 1:
        month = f"0{month}"
    elif len(month) == 2:
        month
    else:
        print("error with the format")
    return month

def correct_date_nippo(df,month):
    timezone_name = "Asia/Tokyo"
    stamp_now = datetime.now(tz=pytz.timezone(timezone_name))
    year = str(stamp_now.year)
    df["day"] = df["day"].map(
        lambda x: f"{year}-{month}-0{x}" if len(str(x)) == 1 else f"{year}-{month}-{x}"
        )
    return df.copy()



def get_shared_bottle(df):
    """"
    input: goukei shosai df
    output: goukei shosai df with shared bottle column and  cp_bottle column as list
    """
    ## the next line is to make sure that the cp_bottle column is a list
    df["cp_bottle"] = df["cp_bottle"].map(lambda x:
                                          x if type(x) == type([]) else
                                          x.split(",") if type(x)==type("") and "," in x else
                                          None if type(x)==type(0.1) else [x] if x != None else None)

    df["shared_bottle"] = df["cp_bottle"].map(lambda x: len(x) if type(x) == type([]) else 1)
    return df.copy()


def add_file_name_to_df(df,file_name):
    df["FILE_NAME"] = file_name
    df["FILE_NAME"] = df["FILE_NAME"].astype("str")
    return df.copy()

def add_date_to_df(df):
    date = get_timestamp("Asia/Tokyo")
    df["DATE"] = date
    df = df.astype({"DATE":"str"})
    return df.copy()

def upload_bq(df,table_id,project_id):
    df.to_gbq(
            destination_table=table_id,
            project_id=project_id,
            progress_bar=False,
            if_exists="append")
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

def clean_assis(df):
    str_col = {
        'store_number': 'str',
        'store_name': 'str',
        'hostess_name': 'str',
        'real_name': 'str',
        'job_category': 'str',
        'cp_code': 'str',
        'payroll_system': 'str',
        'guaranteed_hourly_rate': 'str',
        'slide_hourly_rate': 'str',
        'earnings_slide': 'str',
        'average_hourly_wage': 'str',
        'start_time': 'str',
        'leave_time': 'str'
    }
    df = df.astype({
        col:str_col.get(col,"float64") for col in df.columns
    })
    for col in str_col.keys():
        df[col] = df[col].apply(lambda x: None if x == "nan" else x)
    return df.copy()

def clean_nippo(df):
    ni_str_col = {
    'weekday':'string',
    'weather':'string',
    'visit_time':'string'
    }
    df = df.astype({
        col:ni_str_col.get(col,"float64") for col in df.columns
        })
    df = df.astype({'day':'int64'})

    for col in ni_str_col.keys():
        df[col] = df[col].apply(lambda x: None if type(x) == type("") and x == "nan" else x)
    return df.copy()

def clean_shosai(df):
    gs_str_col = {
    'business_day':'str',
    'order_number':'str',
    'bill_number':'str',
    'order_code':'str',
    'product_name':'str',
    'cp_code_bottle':'str',
    'cp_bottle':'str',
    'cp_in_charge':'str',
    }
    df = df.astype({
    col:gs_str_col.get(col,"float64") for col in df.columns})
    for col in gs_str_col.keys():
        df[col] = df[col].apply(lambda x: None if x == "nan" and type(x)==type("") else x)
    return df.copy()

def clean_goukei_data(df):
    gd_str_col = {
        'order_number':'str',
        'bill_number':'str',
        'business_day':'str',
        'visit_time':'str',
        'bill_date':'str',
        'hour':'str',
        'customer_name':'str',
        'table_name':'str',
        'hostess_name':'str'
    }
    df.astype(
        {col:gd_str_col.get(col,"float64") for col in df.columns}
    )
    for col_str in gd_str_col.keys():
        df[col_str] = df[col_str].apply(lambda x: None if type(x) == type("") and x == "nan" else x)
    return df.copy()


def load_file(path,file_name):
    file_type = identify_file(file_name)
    file_path = f"{path}/{file_name}"
    try:
        if file_type == "assis":
            df = pd.read_excel(file_path,index_col=False,skipfooter=1,engine="openpyxl")
            df = df[assis_jp_en.keys()]
            df.columns = [assis_jp_en[col] for col in df.columns]
            df = clean_assis(df)
            df = add_file_name_to_df(df,file_name)
            df = add_date_to_df(df)
            df.insert(0,"DAY",get_date_from_file(file_name))
            df["DAY"] = df["DAY"].astype("str")
            return df

        elif file_type == "nippo":
            df = pd.read_excel(file_path,index_col=False,skipfooter=2,engine="openpyxl")
            df = df[nippo_jp_en.keys()]
            df.columns = [nippo_jp_en[col] for col in df.columns]
            df = clean_nippo(df)
            df = add_file_name_to_df(df,file_name)
            df = add_date_to_df(df)
            month = get_month_nippo(file_name)
            df = correct_date_nippo(df,month).dropna(axis=0)
            return df.copy().astype({"visit_time":"str"})
            # for col in df.columns:
            #     df[col].map(lambda x: print(f"{x} , {col} {type(x)}") if type(x) not in [type(""),type(1),type(1.1),""] else None)
            # return df

        elif file_type == "shosai":
            df = pd.read_excel(file_path,index_col=False,engine="openpyxl")
            df = df[gs_jp_en.keys()]
            df.columns = [gs_jp_en[col] for col in df.columns]
            df = clean_shosai(df)
            df = add_file_name_to_df(df,file_name)
            df = add_date_to_df(df)

            return df

        elif file_type == "goukei data":
            df = pd.read_excel(file_path,index_col=False,skiprows=5,engine="openpyxl")
            df = df[gd_jp_en.keys()]
            df.columns = [gd_jp_en[col] for col in df.columns]
            df = clean_goukei_data(df)
            df = add_file_name_to_df(df,file_name)
            df = add_date_to_df(df)
            return df
        else:
            print(file_type)
            return print(f"{file_name} unknown file type")
    except Exception as e:
        print(e)
        return print("error loading file")
