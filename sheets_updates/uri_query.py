import os
import gspread
import pandas as pd
import google.auth
from google.cloud import bigquery

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET = os.environ["DATASET"]
TABLE_3 = os.environ["TABLE_3"]
TABLE_4 = os.environ["TABLE_4"]
PRODUCT_1 = os.environ["PRODUCT_1"]
PRODUCT_2 = os.environ["PRODUCT_2"]
PRODUCT_3 = os.environ["PRODUCT_3"]
PRODUCT_M = os.environ["PRODUCT_M"]


def delete_if_exist(month,year=2023):
    month_str = (2-len(str(month)))*"0" + str(month) ## Add a zero if the month is less than 10
    client = bigquery.Client()
    query = f"""
    ----- query for getting the price year_month ------
    DELETE `{DATASET}.{PRODUCT_M}`
    WHERE CAST(year_month AS STRING) = '{year}{month_str}'
    """

    try:
        query_job = client.query(query)
        results = query_job.result()
    except Exception as e:
        print(e)
        return False
    return True


def get_prices_year_db(month,year=2023):
    month_str = (2-len(str(month)))*"0" + str(month) ## Add a zero if the month is less than 10

    client = bigquery.Client()
    query = f"""
    ----- query for getting the price year_month ------
    SELECT distinct year_month
    FROM `{DATASET}.{PRODUCT_M}`
    WHERE CAST(year_month AS STRING) = '{year}{month_str}'
    """

    try:
        query_job = client.query(query)
        results = query_job.result()
        ym_list = [year_month["year_month"] for year_month in results]

    except Exception as e:
        print(e)
        return False
    return ym_list


def get_prices_sh(sheet_name):

  SHEET_ID = os.environ["SHEET_ID"]
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
    prices_master = gc.open_by_key(SHEET_ID)

    ws = prices_master.worksheet(sheet_name)
    df =pd.DataFrame(ws.get_all_values())
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df.columns = [products_dict[col] for col in df.columns]
    df = df[df["product_code"]!= ""].copy()
    integer_cols = {
    "product_code":"int64",
    "selling_price":"int64",
    "cost":"int64",
    "back":"int64",
    "year_month":"int64"
    }
    ### Clean the data

    for col in integer_cols.keys():
        df[col] = df[col].map(lambda x: x.replace(",","").replace(" ","") if isinstance(x,str) else x)
        df[col] = df[col].map(lambda x: 0 if isinstance(x,str) and x == "" else x)
    df = df.astype({f'{col}':f'{integer_cols.get(col,"string")}' for col in df.columns})
    for col in df.columns:
        print("a")
        if col not in integer_cols.keys():
            df[col] = df[col].map(lambda x: None if isinstance(x,str) and x == "" else x)

    ### Check if the year_month is already in the database
    ym_values_df = df["year_month"].unique()
    already_up = get_prices_year_db(month=9)
    try:
        for val in ym_values_df:
            if val in already_up:
                print("This year_month is already in the database")
                ext_month = str(val)[4:6]
                delete_if_exist(month=ext_month)
    except:
        pass
    df.to_gbq(
      destination_table=f"{DATASET}.{PRODUCT_M}",
      project_id=PROJECT_ID,
      progress_bar=False,
      if_exists="append")

    print("Using sheets prices info")
    return True
  except:
    return False


def get_query(existing_values,month="09",type_sh="undefined"):
    month_str = (2-len(str(month)))*"0" + str(month) ## Add a zero if the month is less than 10
    try:
        existing_values.remove('business_day')
    except:
        existing_values = [""]

    list_values = f"""({','.join([f"'{val}'" for val in existing_values])})"""
    if list_values == '()':
        list_values = "('')"

    use_new = get_prices_sh("new")

    if use_new == False:
        print(f"Using saved table {PRODUCT_3}")
    elif use_new == True:
        PRODUCT_3 = PRODUCT_M

    main_q = f"""

    WITH cosa as (
    WITH uri as (WITH totales as (with asv as (
      WITH joined as (
        select sh.*
        EXCEPT(FILE_NAME,DATE_UPLOADED),
        CASE
        WHEN product_name like "CR%" THEN REGEXP_EXTRACT(product_name, r'\(([^)]+)\)')
        WHEN product_name like "DH%" THEN REGEXP_EXTRACT(product_name, r'\(([^)]+)\)')
        WHEN product_name like "HC%" THEN REGEXP_EXTRACT(product_name, r'\(([^)]+)\)')
        ELSE cp_in_charge
        END AS cp_code,
        prod.code as product_classification,
        CASE
          WHEN prod.commission > 0 THEN (prod.commission*number_units)/shared_bottle
          ELSE null
        END as sale_commission,
        prod.commission as raw_commission,
        table_name,
        sh.FILE_NAME,
        ROW_NUMBER() OVER (PARTITION BY sh.order_number ORDER BY business_day) AS occurrence_number,
        CASE
          WHEN ROW_NUMBER() OVER (PARTITION BY sh.order_number ORDER BY business_day) = 1 THEN gd.correction_amount
          ELSE NULL
        END as CORRECTION_VAL,
        CASE
          WHEN ROW_NUMBER() OVER (PARTITION BY sh.order_number ORDER BY business_day) = 1 THEN gd.paid_amount
          ELSE NULL
        END as PAID_AMOUNT,

        CASE
          WHEN ROW_NUMBER() OVER (PARTITION BY sh.order_number ORDER BY business_day) = 1 THEN gd.credit_paid_amount
          ELSE NULL
        END as CREDIT_CARD,

        CASE
          WHEN ROW_NUMBER() OVER (PARTITION BY sh.order_number ORDER BY business_day) = 1 THEN gd.cash_paid_amount
          ELSE NULL
        END as CASH
        from `{DATASET}.{TABLE_3}` as sh
        LEFT JOIN (
        ------ here probably need to change when we define the correct commission table
          SELECT
          CAST(product_code as STRING) as product_code,
          nomenclatore as code,
          CAST(back as INT64) as commission,
          CAST(year_month AS STRING) AS year_month
          FROM `{PROJECT_ID}.{DATASET}.{PRODUCT_3}`

        ) as prod on sh.order_code = prod.product_code
--            AND sh.year_month LIKE CONCAT(prod.year_month,'%')
        LEFT JOIN (
          SELECT distinct
          CAST(order_number as STRING) as order_number_gd,
          CAST(business_day as STRING) as business_str,
          table_name,
          correction_amount,
          paid_amount,
          credit_paid_amount,
          cash_paid_amount

          from `{DATASET}.{TABLE_4}` ) ---- Correct bq table name to work with this query
          as gd on sh.order_number = gd.order_number_gd and gd.business_str = sh.business_day

    )

    SELECT
    *,
    CASE
      WHEN joined.product_name = '割引額（税サ込み）' THEN total_amount/1.1
      WHEN joined.product_name like 'Tip%ON%' THEN total_amount/1.1
      ELSE total_amount
    END as AMOUNT,

    CASE
      WHEN joined.product_name = '割引額（税サ込み）' THEN 0
      WHEN joined.product_name like 'Tip%ON%' THEN 0
      ELSE total_amount*0.2
    END AS SERVICE,

    CASE
      WHEN joined.product_name = '割引額（税サ込み）' THEN (total_amount/1.1)*0.1
      WHEN joined.product_name like 'Tip%ON%' THEN (total_amount/1.1)*0.1
      WHEN joined.product_name not like '%.ON' THEN (total_amount*0.2+total_amount)*0.1
      WHEN joined.product_name like '%.ON' THEN (total_amount)*0.1
      ELSE 0
      -- ELSE total_amount*0.2
    END AS VAT,
    -- ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY business_day) AS occurrence_number

    FROM joined
    -- WHERE business_day like '202309%'
    )
    SELECT asv.*,
    CASE WHEN product_name = '割引額（税サ込み）' THEN asv.total_amount
    ELSE AMOUNT + SERVICE + VAT
    END as SUBTOTAL,
    asv_2.SALES_SUBTOTAL,
    asv_2.SERVICE_CHARGE,
    asv_2.SALES_VAT,

    FROM asv left join (
      SELECT
      business_day,
      order_number,
      SUM(AMOUNT) as SALES_SUBTOTAL,
      SUM(SERVICE) as SERVICE_CHARGE,
      SUM(VAT) as SALES_VAT,
      1 as first_oc
      FROM asv
      GROUP BY
      business_day,
      order_number
      ) as asv_2
      on asv.business_day = asv_2.business_day and asv.order_number = asv_2.order_number and occurrence_number = first_oc
    )

    SELECT

    business_day,
    order_number,
    bill_number,
    sequence,
    `set`,
    order_classification,
    order_code,
    product_name,
    unit_price,
    number_units,
    total_amount,
    cp_bottle,
    cp_code_bottle,
    cp_in_charge,
    shared_bottle,
    cp_code,
    product_classification,
    sale_commission,
    raw_commission,
    table_name,
    occurrence_number,
    AMOUNT,
    SERVICE,
    VAT,
    SUBTOTAL,
    SALES_SUBTOTAL,
    SERVICE_CHARGE,
    SALES_VAT,
    CASE WHEN occurrence_number = 1 THEN SUM(SALES_SUBTOTAL)
    ELSE NULL
    END AS TOTAL,
    CASE WHEN occurrence_number = 1 THEN PAID_AMOUNT-(SALES_SUBTOTAL+SERVICE_CHARGE+SALES_VAT)
    ELSE NULL
    END AS ADJUST,
    --CORRECTION_VAL,
    PAID_AMOUNT,
    CREDIT_CARD,
    CASH,

    FILE_NAME,

    FROM totales
    GROUP BY

    business_day,
    order_number,
    bill_number,
    sequence,
    `set`,
    order_classification,
    order_code,
    product_name,
    unit_price,
    number_units,
    total_amount,
    cp_bottle,
    cp_code_bottle,
    cp_in_charge,
    shared_bottle,
    cp_code,
    product_classification,
    sale_commission,
    raw_commission,
    table_name,
    occurrence_number,
    AMOUNT,
    SERVICE,
    VAT,
    SUBTOTAL,
    SALES_SUBTOTAL,
    SERVICE_CHARGE,
    SALES_VAT,
    --CORRECTION_VAL,
    PAID_AMOUNT,
    CREDIT_CARD,
    CASH,
    FILE_NAME
    )
    """
    uri_part = f"""
    ----- URI QUERY -----
    SELECT

    business_day,
    FORMAT_TIMESTAMP('%m月 %d日 %a', PARSE_DATE('%Y%m%d', business_day)) AS date_day,
    --FORMAT_TIMESTAMP('%a %b %d 日', PARSE_DATE('%Y%m%d', business_day)) AS date_day,
    SUM(SALES_SUBTOTAL) as sales_subtotal,
    SUM(SERVICE_CHARGE) as service_charge,
    SUM(SALES_VAT) as sales_vat,
    SUM(ADJUST) as adjust,
    SUM(TOTAL) as total,
    SUM(PAID_AMOUNT) as payment_amount,
    SUM(CREDIT_CARD) as credit_card,
    SUM(CASH) as cash,
    CASE
      WHEN SUM(CREDIT_CARD) <> 0 THEN CONCAT(REGEXP_EXTRACT(cast(SUM(CASH)/SUM(CREDIT_CARD) as string), r'\d*\.\d{{2}}'), ' %')
      ELSE NULL
    END as card_ratio,
    SUM(CASE WHEN product_classification like '%EN%' THEN number_units ELSE 0 END) AS customers,
    CASE
      WHEN SUM(CASE WHEN product_classification like '%EN%' THEN number_units ELSE 0 END) <> 0 THEN SUM(PAID_AMOUNT)/SUM(CASE WHEN product_classification like '%EN%' THEN number_units ELSE 0 END)
      ELSE NULL
    END AS average,

    -------- START NOT T Tables --------
    SUM(CASE WHEN table_name not like '%T%' THEN SALES_SUBTOTAL ELSE 0 END) as sales_subtotal_not_t,
    SUM(CASE WHEN table_name not like '%T%' THEN SERVICE_CHARGE ELSE 0 END) as service_charge_not_t,
    SUM(CASE WHEN table_name not like '%T%' THEN SALES_VAT ELSE 0 END) as sales_vat_not_t,
    SUM(CASE WHEN table_name not like '%T%' THEN ADJUST ELSE 0 END) as adjust_not_t,
    SUM(CASE WHEN table_name not like '%T%' THEN TOTAL ELSE 0 END) as total_not_t,
    SUM(CASE WHEN table_name not like '%T%' THEN PAID_AMOUNT ELSE 0 END) as payment_amount_not_t,
    SUM(CASE WHEN table_name not like '%T%' THEN CREDIT_CARD ELSE 0 END) as credit_card_not_t,
    SUM(CASE WHEN table_name not like '%T%' THEN CASH ELSE 0 END) as cash_not_t,
    CASE
      WHEN SUM(CASE WHEN table_name not like '%T%' THEN CREDIT_CARD ELSE 0 END) <> 0 THEN CONCAT(REGEXP_EXTRACT(cast(SUM(CASE WHEN table_name not like '%T%' THEN CASH ELSE 0 END)/SUM(CASE WHEN table_name not like '%T%' THEN CREDIT_CARD ELSE 0 END) as string), r'\d*\.\d{{2}}'), ' %')
      ELSE NULL
    END as card_ratio_not_t,
    SUM(CASE WHEN product_classification like '%EN%' and table_name not like '%T%' THEN number_units ELSE 0 END) AS customers_not_t,
    CASE
      WHEN SUM(CASE WHEN product_classification like '%EN%' and table_name not like '%T%' THEN number_units ELSE 0 END) <> 0 THEN SUM(CASE WHEN table_name NOT LIKE '%T%' THEN PAID_AMOUNT ELSE 0 END)/SUM(CASE WHEN product_classification like '%EN%' and table_name not like '%T%' THEN number_units ELSE 0 END)
      ELSE NULL
    END AS average_not_t,
    -------- END NOT T Tables --------

    -------- START T Tables --------
    SUM(CASE WHEN table_name like '%T%' THEN SALES_SUBTOTAL ELSE 0 END) as sales_subtotal_t,
    SUM(CASE WHEN table_name like '%T%' THEN SERVICE_CHARGE ELSE 0 END) as service_charge_t,
    SUM(CASE WHEN table_name like '%T%' THEN SALES_VAT ELSE 0 END) as sales_vat_t,
    SUM(CASE WHEN table_name like '%T%' THEN ADJUST ELSE 0 END) as adjust_t,
    SUM(CASE WHEN table_name like '%T%' THEN TOTAL ELSE 0 END) as total_t,
    SUM(CASE WHEN table_name like '%T%' THEN PAID_AMOUNT ELSE 0 END) as payment_amount_t,
    SUM(CASE WHEN table_name like '%T%' THEN CREDIT_CARD ELSE 0 END) as credit_card_t,
    SUM(CASE WHEN table_name like '%T%' THEN CASH ELSE 0 END) as cash_t,
    CASE
      WHEN SUM(CASE WHEN table_name like '%T%' THEN CREDIT_CARD ELSE 0 END) <> 0 THEN CONCAT(REGEXP_EXTRACT(cast(SUM(CASE WHEN table_name like '%T%' THEN CASH ELSE 0 END)/SUM(CASE WHEN table_name like '%T%' THEN CREDIT_CARD ELSE 0 END) as string), r'\d*\.\d{{2}}'), ' %')
      ELSE NULL
    END as card_ratio_t,
    SUM(CASE WHEN product_classification like '%EN%' and table_name like '%T%' THEN number_units ELSE 0 END) AS customers_t,
    CASE
      WHEN SUM(CASE WHEN product_classification like '%EN%' and table_name like '%T%' THEN number_units ELSE 0 END) <> 0 THEN SUM(CASE WHEN table_name LIKE '%T%' THEN PAID_AMOUNT ELSE 0 END)/SUM(CASE WHEN product_classification like '%EN%' and table_name like '%T%' THEN number_units ELSE 0 END)
      ELSE NULL
    END AS average_t,
    -------- END T Tables --------

    from uri
    group by
    business_day
    )
    SELECT * from cosa
    WHERE business_day not in {list_values}
    AND CAST(business_day as STRING) like CONCAT({year_present},CAST({month_str} AS STRING),"%")
    ORDER BY CAST(business_day as INT64) asc
    """

    year_present = "2023"
    data_part = f"""
    ----- DATA QUERY -----
    SELECT * FROM uri)
    SELECT * from cosa
    WHERE business_day not in {list_values}
    AND CAST(business_day as STRING) like CONCAT({year_present},CAST({month_str} AS STRING),"%")
    ORDER BY CAST(business_day as INT64) asc, CAST(order_number as INT64) asc,CAST(occurrence_number as INT64) asc
    """

    if type_sh == "uri":
        query = main_q + uri_part
    elif type_sh == "data":
        query = main_q + data_part
    else:
        return
    return query
