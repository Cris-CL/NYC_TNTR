import os
def get_uri_query(existing_values,date_start=0):

    try:
        existing_values = existing_values[1:]
    except:
        existing_values = [""]

    list_values = f"""({','.join([f"'{val}'" for val in existing_values])})"""
    if list_values == '()':
        list_values = "('')"

    PROJECT_ID = os.environ["PROJECT_ID"]
    DATASET = os.environ["DATASET"]
    # TABLE_1 = os.environ["TABLE_1"]
    # TABLE_2 = os.environ["TABLE_2"]
    TABLE_3 = os.environ["TABLE_3"]
    TABLE_4 = os.environ["TABLE_4"]
    PRODUCT_1 = os.environ["PRODUCT_1"]
    PRODUCT_2 = os.environ["PRODUCT_2"]

    query = f"""

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
          product_code,
          CODE as code,
          CAST(back as INT64) as commission
          FROM `{PROJECT_ID}.{DATASET}.{PRODUCT_1}`
          UNION ALL

          SELECT
          set_code as product_code,
          CODE as code,
          CAST(REF_1 as INT64) as commission
          FROM `{PROJECT_ID}.{DATASET}.{PRODUCT_2}`

        ) as prod on sh.order_code = prod.product_code
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
      ELSE total_amount
    END as AMOUNT,

    CASE
      WHEN joined.product_name = '割引額（税サ込み）' THEN 0
      WHEN joined.product_name like '%ON)' THEN 0
      ELSE total_amount*0.2
    END AS SERVICE,

    CASE
      WHEN joined.product_name = '割引額（税サ込み）' THEN (total_amount/1.1)*0.1
      WHEN joined.product_name not like '%ON)' THEN (total_amount*0.2+total_amount)*0.1
      WHEN joined.product_name like '%ON)' THEN (total_amount)*0.1
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
    -- SUM(CASE WHEN client_type = 'type_1' THEN sale_amount ELSE 0 END) AS type_1_sales,
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
    AND CAST(business_day as INT64) >= CAST({date_start} AS INT64)
    ORDER BY CAST(business_day as INT64) asc
    """

    return query
