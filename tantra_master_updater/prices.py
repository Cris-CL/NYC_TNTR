import os
import pandas as pd
import gspread
import google.auth
from datetime import datetime
from google.cloud import bigquery

PROJECT_ID = os.environ["PROJECT_ID"]
PRICES_SHEET_ID = os.environ["PRICES_SHEET_ID"]
DATASET = os.environ["DATASET"]
PRODUCT_M = os.environ["PRODUCT_M"]


def replace_prices(year_month):

    client = bigquery.Client()
    query = f"""
        DELETE
        `{DATASET}.{PRODUCT_M}`
        WHERE year_month = {year_month} and UPLOADED < (
        SELECT MAX(UPLOADED)
        FROM `{DATASET}.{PRODUCT_M}`
        WHERE year_month = {year_month}
        )
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        print("Old prices deleted")
    except Exception as e:
        raise e
    return


def update_prices(sheet_name):
  if sheet_name == "MASTER":
    return
  products_dict = {
        '商品コード':'product_code',
        '商品名':'product_name',
        '商品名（かな）':'product_name_kana',
        '商品種別':'product_type',
        '商品詳細種別':'by_product_details',
        '商品単位区分':'product_units_classification',
        '売単価':'selling_price',
        '消費税区分':'consumption_tax_zone',
        'サービス料区分':'service_fee_category',
        '原価':'cost',
        'バック':'back',
        'nomenclature':'nomenclatore',
        'セット種別':'set_type',
        '基準時間':'standard_time',
        'class':'class',
        'year_month':'year_month'
        }

  credentials, _ = google.auth.default()
  gc = gspread.authorize(credentials)
  try:
    prices_master = gc.open_by_key(PRICES_SHEET_ID)

    ws = prices_master.worksheet(sheet_name)

    df =pd.DataFrame(ws.get_all_values())

    df.columns = df.iloc[0]

    df = df.iloc[1:]
    df.columns = [products_dict[col] for col in df.columns]
    df = df[df["product_code"]!= ""].copy()
    integer_cols = {
    "product_code":"int64",
    "selling_price":"int64",
    "standard_time":"int64",
    "cost":"int64",
    "back":"int64",
    "year_month":"int64"
    }

    for col in integer_cols.keys():
        df[col] = df[col].map(lambda x: x.replace(",","").replace(" ","") if isinstance(x,str) else x)
        df[col] = df[col].map(lambda x: 0 if isinstance(x,str) and x == "" else x)
    df = df.astype({f'{col}':f'{integer_cols.get(col,"string")}' for col in df.columns})
    for col in df.columns:
        if col not in integer_cols.keys():
            df[col] = df[col].map(lambda x: None if isinstance(x,str) and x == "" else x)

    df["UPLOADED"] = datetime.now()
    df = df[df['product_code'].notna()]

    year_mon = df["year_month"].unique()[0]
    # return df
    df.to_gbq(
      destination_table=f"{DATASET}.{PRODUCT_M}",
      project_id=PROJECT_ID,
      progress_bar=False,
      if_exists="append")
    replace_prices(year_mon)
    return True
  except Exception as e:
    print(e)
    return False
