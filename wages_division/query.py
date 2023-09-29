import os
from google.cloud import bigquery

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET = os.environ["DATASET"]
TABLE_1 = os.environ["TABLE_1"]
TABLE_2 = os.environ["TABLE_2"]
TABLE_3 = os.environ["TABLE_3"]
TABLE_4 = os.environ["TABLE_4"]
PRODUCT_1 = os.environ["PRODUCT_1"]
PRODUCT_2 = os.environ["PRODUCT_2"]
PRODUCT_3 = os.environ["PRODUCT_3"]


def get_comission_list(month,year=2023):
    month_str = (2-len(str(month)))*"0" + str(month) ## Add a zero if the month is less than 10

    client = bigquery.Client()
    query = f"""
    ----- query for getting the comission list ------
    SELECT distinct nomenclatore as code
    FROM `{PROJECT_ID}.{DATASET}.{PRODUCT_3}`
    WHERE nomenclatore is not null
    AND CAST(year_month AS STRING) = {year}{month_str}
    """

    try:
        query_job = client.query(query)
        results = query_job.result()
        com_lis = [code["code"] for code in results]

    except Exception as e:
        print(e)
        print("Using default comission list")
        com_lis = ['DR1','DR2','DR3','BT','FD','KA','OT','TB','TP','EN','EX','CR','DH','HC']
    return com_lis

def create_query(month,year=2023):

    lis_comission = get_comission_list(month,year)
    full_query = f"""WITH part_one as (
    ----------------START PART ONE----------------
    WITH wages as (with assist as (SELECT DISTINCT
    DAY,
    store_number,
    store_name,
    assis.hostess_name,
    real_name,
    job_category,
    assis.cp_code,
    payroll_system,
    guaranteed_hourly_rate,
    slide_hourly_rate,
    earnings_slide,
    average_hourly_wage,
    working_days,
    working_hours,
    start_time,
    leave_time,
    absence,
    absence_amount,
    late_comming,
    late_amount,
    earnings,
    earnings_subtotal,
    earnings_excluding_tax,
    PARSE_TIME('%H%M', start_time) AS start_time_p,
    PARSE_TIME('%H%M', leave_time) AS leave_time_p,

    CASE
        WHEN start_time <= leave_time THEN
        ROUND(TIMESTAMP_DIFF(
            PARSE_TIMESTAMP('%H%M', leave_time),
            PARSE_TIMESTAMP('%H%M', start_time),
            HOUR
        ) +
        MOD(TIMESTAMP_DIFF(
            PARSE_TIMESTAMP('%H%M', leave_time),
            PARSE_TIMESTAMP('%H%M', start_time),
            MINUTE), 60) / 60.0,2)
        ELSE
        ROUND(24 + TIMESTAMP_DIFF(
            PARSE_TIMESTAMP('%H%M', leave_time),
            PARSE_TIMESTAMP('%H%M', start_time),
            HOUR
        ) +
        MOD(TIMESTAMP_DIFF(
            PARSE_TIMESTAMP('%H%M', leave_time),
            PARSE_TIMESTAMP('%H%M', start_time),
            MINUTE), 60) / 60.0,2)
    END AS work_time_calculated,


    CASE
    WHEN
        PARSE_TIMESTAMP('%H%M', start_time) between  PARSE_TIMESTAMP('%H%M', "0000") AND PARSE_TIMESTAMP('%H%M', "0800")  then DATETIME_ADD(PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY as INT64)," ",start_time)), INTERVAL 1 DAY)
        ELSE
        PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY as INT64)," ",start_time))
    END AS start_datetime,
    CASE
    WHEN
        PARSE_TIMESTAMP('%H%M', leave_time) between  PARSE_TIMESTAMP('%H%M', "0000") AND PARSE_TIMESTAMP('%H%M', "0800")  then DATETIME_ADD(PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY as INT64)," ",leave_time)), INTERVAL 1 DAY)
        ELSE
        PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY as INT64)," ",leave_time))
    END AS leave_datetime,
    wa.hour_a,
    wa.hour_b,

    FROM {DATASET}.{TABLE_1} as assis
    LEFT JOIN {DATASET}.{TABLE_2} as wa
    on wa.hostess_name = assis.hostess_name

    order by CAST(DAY as INT64) ASC

    )
    SELECT
    *,
    CASE
        WHEN leave_datetime <= CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME)
        THEN ROUND(datetime_diff(leave_datetime, start_datetime, MINUTE) / 15,0) * 15 / 60
        WHEN start_datetime < CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME)
        AND leave_datetime >= CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME)
        THEN
            ROUND(datetime_diff(CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME),start_datetime, MINUTE)/60,2)
        ELSE 0
    END as a_wage,

    CASE
    WHEN leave_datetime >= CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME)
        THEN
        ROUND(datetime_diff(leave_datetime,CAST(CONCAT(CAST(DATETIME_ADD(start_datetime, interval 1 DAY) as DATE)," ",PARSE_TIME('%H%M', "0100")) AS DATETIME), MINUTE)/60,2)
    ELSE 0
    END as b_wage
    from assist
    order by CAST(DAY as INT64) ASC
    )
    SELECT
    *,
    hour_a*a_wage + hour_b*b_wage as wage_total_daily

    from wages
    order by CAST(DAY as INT64) ASC, cp_code asc
    ----------------END PART ONE----------------
    )

    SELECT
    part_one.*,
    part_two.*

    from part_one
    LEFT JOIN (
    ----------------START PART TWO----------------
    with sh_piv as (with sho_filtered as (WITH sho_proc as (select sh.*
    EXCEPT(FILE_NAME,DATE_UPLOADED),
    CASE
    WHEN product_name like "CR%" THEN REGEXP_EXTRACT(product_name, r'\(([^)]+)\)')
    WHEN product_name like "DH%" THEN REGEXP_EXTRACT(product_name, r'\(([^)]+)\)')
    WHEN product_name like "HC%" THEN REGEXP_EXTRACT(product_name, r'\(([^)]+)\)')
    ELSE cp_in_charge
    END AS cp_code,
    prod.code,
    CASE
    WHEN prod.commission > 0 THEN (prod.commission*number_units)/shared_bottle
    ELSE null
    END as sale_commission,
    prod.commission as raw_commission,
    table_name

    from {DATASET}.{TABLE_3} as sh
    LEFT JOIN (

    ------ query for getting the comission list ------
    SELECT
    CAST(product_code as STRING) as product_code,
    nomenclatore as code,
    CAST(back as INT64) as commission
    FROM `{PROJECT_ID}.{DATASET}.{PRODUCT_3}`

    ) as prod on sh.order_code = prod.product_code
    LEFT JOIN (
    SELECT distinct
    CAST(order_number as STRING) as order_number,
    table_name
    from {DATASET}.`{TABLE_4}` )
    as gd on sh.order_number = gd.order_number

    ORDER by CAST(business_day as INT64) asc,order_number asc)

    SELECT
    business_day,
    CASE
    WHEN cp_in_charge is null THEN SPLIT(cp_code,",")
    ELSE SPLIT(cp_in_charge,",")
    end as cp_final,

    code,
    number_units,
    shared_bottle,
    sale_commission,
    raw_commission,
    table_name

    from
    sho_proc
    where raw_commission > 0
    )
    select

    business_day,
    flat_names,
    code,
    sum(raw_commission) as total

    from sho_filtered
    cross join unnest(sho_filtered.cp_final) as flat_names
    group by
    business_day,
    flat_names,
    code
    )
    SELECT
    *
    from sh_piv
    pivot (
    sum(sh_piv.total) as commision
    FOR code in {str(lis_comission).replace("[","(").replace("]",")")}
    )
    order by business_day asc, flat_names asc
    )
    ----------------END PART TWO----------------

    as part_two on CAST(part_one.DAY AS STRING) = CAST(part_two.business_day AS STRING) and part_two.flat_names = part_one.hostess_name
    WHERE EXTRACT(MONTH FROM start_datetime) = {month} and EXTRACT(YEAR FROM start_datetime) = {year}
    ORDER BY CAST(DAY AS INT64) asc, cp_code asc

    """
    return full_query
