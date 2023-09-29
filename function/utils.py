import pytz
import os
import re
import pandas as pd
from google.cloud import bigquery,storage
from datetime import datetime
from assis import assis_jp_en
from goukei_data import gd_jp_en
from goukei_shosai import gs_jp_en
from nippo import nippo_jp_en


def clean_dict(dict_in):
    dict_out = {}
    for key in dict_in.keys():
        new_key = key.replace(" ","")
        dict_out[new_key] = dict_in[key]
    return dict_out

assis_jp_en = clean_dict(assis_jp_en)
gd_jp_en = clean_dict(gd_jp_en)
gs_jp_en = clean_dict(gs_jp_en)
nippo_jp_en = clean_dict(nippo_jp_en)


def get_timestamp(timezone_name):
  dt = datetime.now(pytz.timezone(timezone_name))
  timestamp = dt.strftime(("%Y-%m-%d %H:%M:%S"))
  return timestamp


def get_date_from_file(file_name):
    ## new way
    date_pattern = r'\d{8}'

    # Search for the date pattern in the file_name
    match = re.search(date_pattern, file_name)

    if match:
        return match.group()
    else:
        try:
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
        except Exception as e:
            print(e)
            date_2 = None
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
    df["shared_bottle"] = df["cp_bottle"].map(lambda x:
                                          x if type(x) == type([]) else
                                          x.split(",") if type(x)==type("") and "," in x else
                                          None if type(x)==type(0.1) else [x] if x != None else None).map(
                                              lambda x: len(x) if type(x) == type([]) else 1)

    return df.copy()


def fix_time_assis(df):

    """this function see the string of each column and verifies that the length is 4
    if not adds a 0 at the beginning until it is 4 in case is empty doesn't do anything"""
    columns_time = ["start_time","leave_time"]
    def fix_time(x):
        if type(x) != type(""):
            return x
        elif len(x) == 0:
            return x
        elif len(x) < 4:
            return "0"*(4-len(x))+str(x)
        else:
            return x
    for col in columns_time:
        df[col] = df[col].map(lambda x: x.replace(".0","") if type(x) == type("") else x)
        df[col] = df[col].map(fix_time)
    return df.copy()


def add_file_name_to_df(df,file_name):
    df["FILE_NAME"] = file_name
    df["FILE_NAME"] = df["FILE_NAME"].astype("str")
    return df.copy()


def add_date_to_df(df):
    date = get_timestamp("Asia/Tokyo")
    df["DATE_UPLOADED"] = date
    df = df.astype({"DATE_UPLOADED":"str"})
    return df.copy()


def identify_file(file_name):
    lower_file_name = file_name.lower()
    if "ass" in lower_file_name:
        return "assis"
    elif "nippo" in lower_file_name:
        return "nippo"
    elif "gsh" in lower_file_name or "ghs" in lower_file_name:
        return "shosai"
    elif "gdt" in lower_file_name or "gtd" in lower_file_name:
        return "goukei_data"
    elif "assis" in lower_file_name:
        return "assis"
    elif "nippo" in lower_file_name:
        return "nippo"
    elif "goukei sh" in lower_file_name:
        return "shosai"
    elif "goukei d" in lower_file_name:
        return "goukei_data"
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
        df[col] = df[col].apply(lambda x: None if x in [
            "nan","none","NAN","NaN",""," "
            ] else x)
    return df.copy()


def clean_nippo(df):
    ni_str_col = {
    'weekday':'string',
    'weather':'string',
    }
    df = df.astype({
        col:ni_str_col.get(col,"float64") for col in df.columns
        })
    df = df.astype({'day':'int64'})

    for col in ni_str_col.keys():
        df[col] = df[col].apply(lambda x: None if type(x) == type("") and x in [
            "nan","none","NAN","NaN",""," "
            ] else x)
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
        df[col] = df[col].apply(lambda x: None if type(x)==type("") and x in[
            "nan","none","NAN","NaN",""," "] else x)
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
    df["visit_time"] = df["visit_time"].map(str)
    df["hostess_name"] = df["hostess_name"].map(str)

    for col_str in gd_str_col.keys():
        df[col_str] = df[col_str].apply(lambda x: None if type(x) == type("") and x.lower().replace(" ","") in [
            "nan","none","NAN","NaN",""," "
            ] else x)
    return df.copy()


def load_file(uri,file_name):
    file_type = identify_file(file_name)
    print(file_name)
    print(uri)
    file_path = uri
    print(f"trying to load file {file_name}")
    try:
        if file_type == "assis":
            df = pd.read_excel(file_path,index_col=False,skipfooter=1,engine="openpyxl")
            df.columns = [col_wrong.replace(" ","") for col_wrong in df.columns]
            df = df[assis_jp_en.keys()]
            df.columns = [assis_jp_en[col] for col in df.columns]
            df = clean_assis(df)
            df = add_file_name_to_df(df,file_name)
            df = add_date_to_df(df)
            df.insert(0,"DAY",get_date_from_file(file_name))
            df["DAY"] = df["DAY"].astype("str")
            df = fix_time_assis(df)
            return df

        elif file_type == "nippo":
            df = pd.read_excel(file_path,index_col=False,skipfooter=2,engine="openpyxl")
            df.columns = [col_wrong.replace(" ","") for col_wrong in df.columns]
            df = df[nippo_jp_en.keys()]
            df.columns = [nippo_jp_en[col] for col in df.columns]
            df = clean_nippo(df)
            df = add_file_name_to_df(df,file_name)
            df = add_date_to_df(df)
            month = get_month_nippo(file_name)
            df = correct_date_nippo(df,month).dropna(axis=0)
            return df.copy()

        elif file_type == "shosai":
            df = pd.read_excel(file_path,index_col=False,engine="openpyxl")
            df.columns = [col_wrong.replace(" ","") for col_wrong in df.columns]
            df = df[gs_jp_en.keys()]
            df.columns = [gs_jp_en[col] for col in df.columns]
            df = clean_shosai(df)
            df = get_shared_bottle(df)
            df = add_file_name_to_df(df,file_name)
            df = add_date_to_df(df)
            return df.copy()

        elif file_type == "goukei_data":
            df = pd.read_excel(file_path,index_col=False,skiprows=5,engine="openpyxl")
            df.columns = [col_wrong.replace(" ","") for col_wrong in df.columns]
            df = df[gd_jp_en.keys()]
            df.columns = [gd_jp_en[col] for col in df.columns]
            df = clean_goukei_data(df)
            df = add_file_name_to_df(df,file_name)
            df = add_date_to_df(df)
            return df.copy()

        else:
            print(file_type)
            return print(f"{file_name} unknown file type")
    except Exception as e:
        print(e)
        return print(f"Error loading file {file_name}")


def get_list_reports(dataset,table,row):
    """
    Given a table name, returns a list with the file names that were already uploaded to bq
    """
    with bigquery.Client() as client:
        try:
            query = f"""
            SELECT DISTINCT {row}
            FROM `test-bigquery-cc.{dataset}.{table}`
            """
            query_job = client.query(query)

            rows = query_job.result()
            list_reports_uploaded = [row_it[row] for row_it in rows]
        except Exception as e:
            print(e)
            list_reports_uploaded = []
    return list_reports_uploaded


def check_exist_db(file_name,dataset,table,row):
    """
    Function
    That checks if the file already exist in the database or not
    """
    try:
        list_files = get_list_reports(dataset,table,row)
    except Exception as e:
        list_files = []
        print(e)

    if file_name in list_files:
        return True
    else:
        return False


def file_exist_already(file_name,dataset,table,row):
    """If the file exist then the file is deleted from the db"""
    with bigquery.Client() as client:
        query = f"""
        DELETE `{dataset}.{table}`
        WHERE {row} = '{file_name}'
        """
        delete_job = client.query(query)
        delete_job.result()
        print(f"{file_name} deleted from {table}")
    return

# df_1.to_gbq(destination_table=f'{dataset_id}.{table_1}',if_exists='append',progress_bar=False)

def upload_bq(df,table_id,project_id):
    try:
        df.to_gbq(
            destination_table=table_id,
            project_id=project_id,
            progress_bar=False,
            if_exists="append")
    except Exception as e:
        print("Error in upload_bq")
        print(e)
    return


def move_file(origin_bucket, file_name, destination_bucket_name, file_name_destination):
    """Moves a blob from one bucket to another with a new name."""
    move_client = storage.Client()
    try:
        source_bucket = move_client.bucket(origin_bucket)
        source_blob = source_bucket.blob(file_name)
        destination_bucket = move_client.bucket(destination_bucket_name)

        source_bucket.copy_blob(
            source_blob, destination_bucket, file_name_destination
        )
        source_bucket.delete_blob(file_name)
        print(f"File {file_name} moved to {destination_bucket_name} with name {file_name_destination}")
    except Exception as e:
        print(f"Error in move_file {file_name} -- type: {type(e)} -- {e}")
    return


def save_processed_file(df,file_name):
    """
    After the file is processed it is saved in a bucket
    """
    PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
    try:
        storage_client = storage.Client()
        file_name = file_name.replace(".xlsx",".csv")
        bucket = storage_client.list_buckets().client.bucket(PROCESSED_BUCKET)
        blob = bucket.blob(file_name)
        blob.upload_from_string(df.to_csv(index=False), content_type="csv/txt")
    except Exception as e:
        print(f"Error at saving processed file for {file_name} -- type: {type(e)} -- {e}")
    return

def full_upload_process(df,file_name):
    PROJECT_ID = os.environ["PROJECT_ID"]
    DATASET = os.environ["DATASET"]
    TABLE_1 = os.environ["TABLE_1"]
    TABLE_2 = os.environ["TABLE_2"]
    TABLE_3 = os.environ["TABLE_3"]
    TABLE_4 = os.environ["TABLE_4"]
    ORIGIN_BUCKET = os.environ["ORIGIN_BUCKET"]
    DESTINATION_BUCKET = os.environ["DESTINATION_BUCKET"]

    upload_dict = {
        "assis":TABLE_1,
        "nippo":TABLE_2,
        "shosai":TABLE_3,
        "goukei_data":TABLE_4,
    }
    try:
        file_type = identify_file(file_name)
        table_upload = upload_dict.get(file_type,"unknown")
        if table_upload == "unknown":
            print("unknown file type")
            return
        row = "FILE_NAME"
        check = check_exist_db(file_name,DATASET,table_upload,row)
    except Exception as e:
        print("Error in full_upload_process check step")
        print(e)
    if check == True:

        file_exist_already(file_name,DATASET,table_upload,row)

        timestamp = get_timestamp("Asia/Tokyo").replace(" ","_").replace("-","_").replace(":","_")
        new_file_name = f'{file_name.split(".")[0]}_{timestamp}.xlsx'
        try:
            move_file(ORIGIN_BUCKET,file_name,DESTINATION_BUCKET,new_file_name)
        except Exception as e:
            print("Error in full_upload_process move step")
            print(e)
    elif check == False:
        print("New file")
        try:
            move_file(ORIGIN_BUCKET,file_name,DESTINATION_BUCKET,file_name)
        except Exception as e:
            print("Error in full_upload_process move step new file")
            print(e)
    dataset_table = f"{DATASET}.{table_upload}"
    upload_bq(df,dataset_table,PROJECT_ID)
    print(f"File {file_name} uploaded to {table_upload}")
    save_processed_file(df,file_name)
    return

def load_and_upload(uri,file_name):
    df = load_file(uri,file_name)
    full_upload_process(df,file_name)
    return
