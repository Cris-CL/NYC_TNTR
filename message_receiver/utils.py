import pytz
import os
import re
import pandas as pd
from google.cloud import bigquery, storage
from datetime import datetime
from parameters_tantra import *


def clean_dict(dict_in):
    dict_out = {}
    for key in dict_in.keys():
        new_key = key.replace(" ", "")
        dict_out[new_key] = dict_in[key]
    return dict_out


assis_jp_en = clean_dict(assis_jp_en)
gd_jp_en = clean_dict(gd_jp_en)
gs_jp_en = clean_dict(gs_jp_en)
nippo_jp_en = clean_dict(nippo_jp_en)


def remove_nas(df):
    try:
        cleaned_df = df.copy()
        for col in cleaned_df.columns:
            cleaned_df[col] = cleaned_df[col].apply(
                lambda x: None
                if isinstance(x, str)
                and x.lower() in ["nan", "none", "<na>", "<nan>", "", " "]
                else x
            )
        return cleaned_df
    except Exception as e:
        print(e)
        print("remove_nas: Couldn't clean the null values")
        return df


def get_timestamp(timezone_name):
    dt = datetime.now(pytz.timezone(timezone_name))
    timestamp = dt.strftime(("%Y-%m-%d %H:%M:%S"))
    return timestamp


def get_date_from_file(file_name):
    ## new way
    date_pattern = r"\d{8}"

    # Search for the date pattern in the file_name
    match = re.search(date_pattern, file_name)

    if match:
        return match.group()
    else:
        try:
            date_ls = (
                file_name.replace("Assis", "")
                .replace(".xlsx", "")
                .replace(" ", "")
                .split("-")
            )
            timezone_name = "Asia/Tokyo"
            stamp_now = datetime.now(tz=pytz.timezone(timezone_name))
            ## day format
            if len(date_ls[0]) == 1:
                date_ls[0] = f"0{date_ls[0]}"
            elif len(date_ls[0]) > 2:
                return print("incorrect day format")
            ## month format
            if len(date_ls[1]) == 1:
                date_ls[1] = f"0{date_ls[1]}"
            elif len(date_ls[1]) > 2:
                return print("incorrect month format")
            # print(date_ls)
            date_2 = f"{stamp_now.year}{date_ls[0]}{date_ls[1]}"
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


def correct_date_nippo(df, month):
    timezone_name = "Asia/Tokyo"
    stamp_now = datetime.now(tz=pytz.timezone(timezone_name))
    year = str(stamp_now.year)
    df["day"] = df["day"].map(
        lambda x: f"{year}-{month}-0{x}" if len(str(x)) == 1 else f"{year}-{month}-{x}"
    )
    return df.copy()


def extract_info(text):
    if isinstance(text, str):
        match = re.search(r"\((.*?)\)", text)
        return match.group(1).replace(" ,", ",").replace(", ", ",") if match else ""
    else:
        print(text, type(text))
        return ""


def get_name_from_order(df):
    df["cp_in_charge"] = df.apply(
        lambda row: extract_info(row["product_name"])
        if pd.isna(row["cp_in_charge"]) or row["cp_in_charge"] == ""
        else row["cp_in_charge"],
        axis=1,
    )
    df["cp_bottle"] = df.apply(
        lambda row: extract_info(row["product_name"])
        if pd.isna(row["cp_bottle"]) or row["cp_bottle"] == ""
        else row["cp_bottle"],
        axis=1,
    )
    return df.copy()


def get_shared_bottle(df):
    """ "
    input: goukei shosai df
    output: goukei shosai df with shared bottle column and  cp_bottle column as list
    """

    ##### CHANGE HERE cp_in_charge instead of cp_bottle
    ## the next line is to make sure that the cp_bottle column is a list
    df["shared_bottle"] = (
        df["cp_in_charge"]
        .map(
            lambda x: x
            if isinstance(x, list)
            else x.split(",")
            if isinstance(x, str) and "," in x
            else None
            if isinstance(x, float)
            else [x]
            if x != None
            else None
        )
        .map(lambda x: len(x) if isinstance(x, list) else 1)
    )

    return df.copy()


def fix_time_assis(df):

    """this function see the string of each column and verifies that the length is 4
    if not adds a 0 at the beginning until it is 4 in case is empty doesn't do anything"""
    columns_time = ["start_time", "leave_time"]

    def fix_time(x):
        if not isinstance(x, str):
            return x
        elif len(x) == 0:
            return x
        elif len(x) < 4:
            return "0" * (4 - len(x)) + str(x)
        else:
            return x

    for col in columns_time:
        df[col] = df[col].map(
            lambda x: x.replace(".0", "").replace("2400", "0000")
            if isinstance(x, str)
            else x
        )
        df[col] = df[col].map(fix_time)
    return df.copy()


def add_file_name_to_df(df, file_name):
    df["FILE_NAME"] = file_name
    df["FILE_NAME"] = df["FILE_NAME"].astype("str")
    return df.copy()


def add_date_to_df(df):
    date = get_timestamp("Asia/Tokyo")
    df["DATE_UPLOADED"] = date
    df = df.astype({"DATE_UPLOADED": "str"})
    return df.copy()


def is_valid_filename(filename):

    # pattern = r'^NYC_\d{8}_(ASS|GSH|GDT)\.xlsx$' ### Previous pattern
    pattern = (
        r"^NYC_(\d{4}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01]))_(ASS|GSH|GDT)\.xlsx$"
    )
    if re.match(pattern, filename):
        return True
    else:
        return False


def identify_file(file_name):
    lower_file_name = file_name.lower()
    if is_valid_filename(file_name):
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
        "store_number": "str",
        "store_name": "str",
        "hostess_name": "str",
        "real_name": "str",
        "job_category": "str",
        "cp_code": "str",
        "payroll_system": "str",
        "guaranteed_hourly_rate": "str",
        "slide_hourly_rate": "str",
        "earnings_slide": "str",
        "average_hourly_wage": "str",
        "start_time": "str",
        "leave_time": "str",
    }
    try:
        df["working_hours"] = df["working_hours"].apply(
            lambda x: x.replace(",", ".") if isinstance(x, str) else x
        )
    except:
        print("Problem with clean_assis trying to convert string to number")
    df = df.astype({col: str_col.get(col, "float64") for col in df.columns})
    df = remove_nas(df)
    return df.copy()


def clean_nippo(df):
    ni_str_col = {
        "weekday": "string",
        "weather": "string",
    }
    df = df.astype({col: ni_str_col.get(col, "float64") for col in df.columns})
    df = df.astype({"day": "int64"})

    df = remove_nas(df)
    return df.copy()


def clean_shosai(df):
    gs_str_col = {
        "business_day": "str",
        "order_number": "str",
        "bill_number": "str",
        "order_code": "str",
        "product_name": "str",
        "cp_code_bottle": "str",
        "cp_bottle": "str",
        "cp_in_charge": "str",
    }
    df = df.astype({col: gs_str_col.get(col, "float64") for col in df.columns})
    df = remove_nas(df)
    cp_columns = [
        "cp_code_bottle",
        "cp_bottle",
        "cp_in_charge",
    ]
    for cp_col in cp_columns:
        #### DELETE whitespace from the lists
        df[cp_col] = df[cp_col].apply(
            lambda x: x.replace(", ", ",").replace(" ,", ",")
            if isinstance(x, str)
            else x
        )
    for col_grl in df.columns:
        df[col_grl] = df[col_grl].apply(
            lambda x: x.replace(".0", "") if isinstance(x, str) else x
        )
    df = df[df["business_day"].notna()]
    return df.copy()


def clean_goukei_data(df):
    gd_str_col = {
        "order_number": "str",
        "bill_number": "str",
        "business_day": "str",
        "visit_time": "str",
        "bill_date": "str",
        "hour": "str",
        "customer_name": "str",
        "table_name": "str",
        "hostess_name": "str",
    }
    df.astype({col: gd_str_col.get(col, "float64") for col in df.columns})
    df["visit_time"] = df["visit_time"].map(str)
    df["hostess_name"] = df["hostess_name"].map(str)

    df = remove_nas(df)
    return df.copy()


def assist_df_process(path, file_name):
    try:
        df = pd.read_excel(path, index_col=False, skipfooter=1, engine="openpyxl")
        df.columns = [col_wrong.replace(" ", "") for col_wrong in df.columns]
        df = df[assis_jp_en.keys()]
        df.columns = [assis_jp_en[col] for col in df.columns]
        df = clean_assis(df)
        df = add_file_name_to_df(df, file_name)
        df = add_date_to_df(df)
        df.insert(0, "DAY", get_date_from_file(file_name))
        df["DAY"] = df["DAY"].astype("str")
        df = fix_time_assis(df)
    except Exception as e:
        print("assist_df_process: Error loading assist df")
        raise e
    return df.copy()


def shosai_df_process(path, file_name):
    try:
        df = pd.read_excel(path, index_col=False, engine="openpyxl")
        df.columns = [col_wrong.replace(" ", "") for col_wrong in df.columns]
        df = df[gs_jp_en.keys()]
        df.columns = [gs_jp_en[col] for col in df.columns]
        df = clean_shosai(df)
        df = get_name_from_order(df)
        df = get_shared_bottle(df)
        df = add_file_name_to_df(df, file_name)
        df = add_date_to_df(df)
    except Exception as e:
        print("shosai_df_process: Error loading shosai df")
        raise e
    return df.copy()


def goukei_df_process(path, file_name):
    try:
        df = pd.read_excel(path, index_col=False, skiprows=5, engine="openpyxl")
        df.columns = [col_wrong.replace(" ", "") for col_wrong in df.columns]
        df = df[gd_jp_en.keys()]
        df.columns = [gd_jp_en[col] for col in df.columns]
        df = clean_goukei_data(df)
        df = add_file_name_to_df(df, file_name)
        df = add_date_to_df(df)
    except Exception as e:
        print("goukei_df_process: Error loading goukei df")
        raise e
    return df.copy()


def nippo_df_process(path, file_name):
    try:
        df = pd.read_excel(path, index_col=False, skipfooter=2, engine="openpyxl")
        df.columns = [col_wrong.replace(" ", "") for col_wrong in df.columns]
        df = df[nippo_jp_en.keys()]
        df.columns = [nippo_jp_en[col] for col in df.columns]
        df = clean_nippo(df)
        df = add_file_name_to_df(df, file_name)
        df = add_date_to_df(df)
        month = get_month_nippo(file_name)
        df = correct_date_nippo(df, month).dropna(axis=0)
    except Exception as e:
        print("nippo_df_process: Error loading nippo df")
        raise e
    return df.copy()


def check_bucket(bucket_name, file_name):
    """
    Function that given a bucket_name returns true if there is a file named
    file_name inside or false if is not.

    Parameters:
    - bucket_name (String) : Name of the bucket to check
    - file_name (String) : Name of the file we want to know if is in the bucket

    Returns:
    - Boolean: True if the filename is contained in the bucket, False otherwise
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        list_names = []
        for blob_data in bucket.list_blobs():
            list_names.append(blob_data.name)
        return file_name in list_names
    except:
        return False


def load_file(uri, file_name):
    file_type = identify_file(file_name)
    file_path = uri
    df = pd.DataFrame()  # Initialize df as an empty DataFrame
    print(f"Trying to load file: {file_name} from the URI: {uri}")
    check = check_bucket(ORIGIN_BUCKET, file_name)
    if not check:
        print(f"File with name: {file_name} is not in {ORIGIN_BUCKET}")
    else:
        try:
            if file_type == "assis":
                df = assist_df_process(file_path, file_name)
            elif file_type == "nippo":
                df = nippo_df_process(file_path, file_name)
            elif file_type == "shosai":
                df = shosai_df_process(file_path, file_name)
            elif file_type == "goukei_data":
                df = goukei_df_process(file_path, file_name)
            else:
                print(f"{file_name} has an unknown file type: {file_type}")

            if not df.empty:  # Only remove NaNs if df is not empty
                df = remove_nas(df)
        except Exception as e:
            print("Error in load_file:", e, type(e))
            print(f"Error loading file {file_name}")
            df = pd.DataFrame()  # Ensure df is an empty DataFrame on error

    return df


def get_list_reports(dataset, table, row):
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
            print("Error get_list_reports: ", e, type(e))
            list_reports_uploaded = []
    return list_reports_uploaded


def check_exist_db(file_name, dataset, table, row):
    """
    Function
    That checks if the file already exist in the database or not
    """
    try:
        list_files = get_list_reports(dataset, table, row)
    except Exception as e:
        list_files = []
        print("Error check_exist_db: ", e, type(e))

    if file_name in list_files:
        return True
    else:
        return False


def file_exist_already(file_name, dataset, table, row):
    """If the file exist then the file is deleted from the db"""
    with bigquery.Client() as client:
        query = f"""
        DELETE `{dataset}.{table}`
        WHERE {row} = '{file_name}'
        AND CAST(DATE_UPLOADED AS DATETIME) < (
            SELECT MAX(CAST(DATE_UPLOADED AS DATETIME))
            FROM `{dataset}.{table}`
            WHERE {row} = '{file_name}'
        )
        """
        delete_job = client.query(query)
        delete_job.result()
        print(f"{file_name} updated from {table}")
    return


def upload_bq(df, table_id, project_id):
    try:
        df.to_gbq(
            destination_table=table_id,
            project_id=project_id,
            progress_bar=False,
            if_exists="append",
        )
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

        source_bucket.copy_blob(source_blob, destination_bucket, file_name_destination)
        source_bucket.delete_blob(file_name)
        print(
            f"File {file_name} moved to {destination_bucket_name} with name {file_name_destination}"
        )
    except Exception as e:
        print(f"Error in move_file {file_name} -- type: {type(e)} -- {e}")
    return


def save_processed_file(df, file_name):
    """
    After the file is processed it is saved in a bucket
    """
    try:
        storage_client = storage.Client()
        file_name = file_name.replace(".xlsx", ".csv")
        bucket = storage_client.list_buckets().client.bucket(PROCESSED_BUCKET)
        blob = bucket.blob(file_name)
        blob.upload_from_string(df.to_csv(index=False), content_type="csv/txt")
    except Exception as e:
        print(
            f"Error at save_processed_file file for {file_name} -- type: {type(e)} -- {e}"
        )
    return


def full_upload_process(df, file_name):
    try:
        file_type = identify_file(file_name)
        table_upload = upload_dict.get(file_type, "unknown")
        if table_upload == "unknown":
            print("unknown file type")
            return
        row = "FILE_NAME"
        check = check_exist_db(file_name, DATASET, table_upload, row)
    except Exception as e:
        print("Error in full_upload_process check_exist_db step")
        print(e, type(e))
    if check:
        timestamp = (
            get_timestamp("Asia/Tokyo")
            .replace(" ", "_")
            .replace("-", "_")
            .replace(":", "_")
        )
        new_file_name = f'{file_name.split(".")[0]}_{timestamp}.xlsx'
        try:
            move_file(ORIGIN_BUCKET, file_name, DESTINATION_BUCKET, new_file_name)
        except Exception as e:
            print("Error in full_upload_process move step")
            print(e)
    else:
        print("New file")
        try:
            move_file(ORIGIN_BUCKET, file_name, DESTINATION_BUCKET, file_name)
        except Exception as e:
            print("Error in full_upload_process move step new file")
            print(e, type(e))
    dataset_table = f"{DATASET}.{table_upload}"
    upload_bq(df, dataset_table, PROJECT_ID)
    file_exist_already(file_name, DATASET, table_upload, row)
    print(f"File {file_name} uploaded to {table_upload}")
    save_processed_file(df, file_name)
    return


def load_and_upload(uri, file_name):
    df = load_file(uri, file_name)
    if df.empty:
        print(f"Problem with file: {file_name}")
        print(uri)
        print("Aborting process")
        return
    full_upload_process(df, file_name)
    return
