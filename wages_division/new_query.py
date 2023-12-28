import os


DATASET = os.environ["DATASET"]
TABLE_1 = os.environ["TABLE_1"]
TABLE_2 = os.environ["TABLE_2"]
TABLE_3 = os.environ["TABLE_3"]
TABLE_4 = os.environ["TABLE_4"]
PRODUCT_3 = os.environ["PRODUCT_3"]
EXTRA_H = os.environ["EXTRA_H"]

def create_new_query(month,year=2023):
    month_str = (2-len(str(month)))*"0" + str(month) ## Add a zero if the month is less than 10
    query = f"""

---- QUERY FROM FUNCTION ----
with tips_div as (
WITH
  total_s AS (
  WITH
    todo AS (
    WITH
      part_one AS (
        ----------------START PART ONE----------------
      WITH
        wages AS (
        WITH
          assist AS (
          SELECT
            DISTINCT
            DAY,
            assis.hostess_name,
            assis.cp_code,
            working_days,
            working_hours,
            start_time,
            leave_time,
            PARSE_TIME('%H%M', start_time) AS start_time_p,
            PARSE_TIME('%H%M', leave_time) AS leave_time_p,
            CASE
              WHEN start_time <= leave_time THEN ROUND(TIMESTAMP_DIFF( PARSE_TIMESTAMP('%H%M', leave_time), PARSE_TIMESTAMP('%H%M', start_time), HOUR ) + MOD(TIMESTAMP_DIFF( PARSE_TIMESTAMP('%H%M', leave_time), PARSE_TIMESTAMP('%H%M', start_time), MINUTE), 60) / 60.0,2)
            ELSE
            ROUND(24 + TIMESTAMP_DIFF( PARSE_TIMESTAMP('%H%M', leave_time), PARSE_TIMESTAMP('%H%M', start_time), HOUR ) + MOD(TIMESTAMP_DIFF( PARSE_TIMESTAMP('%H%M', leave_time), PARSE_TIMESTAMP('%H%M', start_time), MINUTE), 60) / 60.0,2)
          END
            AS work_time_calculated,
            CASE
              WHEN PARSE_TIMESTAMP('%H%M', start_time) BETWEEN PARSE_TIMESTAMP('%H%M', "0000") AND PARSE_TIMESTAMP('%H%M', "1000") THEN DATETIME_ADD(PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY AS INT64)," ",start_time)), INTERVAL 1 DAY)
            ELSE
            PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY AS INT64)," ",start_time))
          END
            AS start_datetime,
            CASE
              WHEN PARSE_TIMESTAMP('%H%M', leave_time) BETWEEN PARSE_TIMESTAMP('%H%M', "0000") AND PARSE_TIMESTAMP('%H%M', "1000") THEN DATETIME_ADD(PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY AS INT64)," ",leave_time)), INTERVAL 1 DAY)
            ELSE
            PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY AS INT64)," ",leave_time))
          END
            AS leave_datetime,
            wa.hour_a,
            wa.hour_b,
            DATETIME_ADD(PARSE_DATETIME("%Y%m%d %H%M",CONCAT(CAST(DAY AS INT64)," ","0100")), INTERVAL 1 DAY) as limit_a_wage
          FROM
            `{DATASET}.{TABLE_1}` AS assis
          LEFT JOIN
            `{DATASET}.{TABLE_2}` AS wa
          ON
            wa.hostess_name = assis.hostess_name AND assis.DAY like CONCAT(wa.YEAR_MONTH,'%'))
        SELECT
        * except(limit_a_wage),
        CASE
          WHEN start_datetime > limit_a_wage THEN 0
          WHEN leave_datetime >= limit_a_wage THEN ROUND(DATETIME_DIFF(limit_a_wage,start_datetime, MINUTE)/60,2)
          WHEN leave_datetime <= limit_a_wage THEN ROUND(DATETIME_DIFF(leave_datetime, start_datetime, MINUTE) / 60,2)
          -- WHEN start_datetime < limit_a_wage AND leave_datetime >= limit_a_wage THEN ROUND(DATETIME_DIFF(limit_a_wage,start_datetime, MINUTE)/60,2)
        ELSE 0
        END
          AS a_wage,
        CASE
          WHEN leave_datetime < limit_a_wage THEN 0
          WHEN start_datetime >= limit_a_wage THEN ROUND(DATETIME_DIFF(leave_datetime,start_datetime, MINUTE)/60,2)
          WHEN start_datetime <= limit_a_wage AND leave_datetime >= limit_a_wage THEN ROUND(DATETIME_DIFF(leave_datetime,limit_a_wage, MINUTE)/60,2)
        ELSE 0
        END
          AS b_wage
        FROM
          assist )
      SELECT
        *,
        hour_a*a_wage + hour_b*b_wage AS wage_total_daily
      FROM
        wages
        ----------------END PART ONE----------------
        )
    SELECT
      part_one.*,
      part_two.*
    FROM
      part_one
    LEFT JOIN (
        ----------------START PART TWO----------------
      WITH
        sh_piv AS (
        WITH
          sho_filtered AS (
          WITH
            sho_proc AS (
            SELECT
              sh.* EXCEPT(FILE_NAME,
                DATE_UPLOADED),
              CASE
                WHEN product_name LIKE "CR%" THEN REGEXP_EXTRACT(product_name, r'\(([^)]+)\)')
                WHEN product_name LIKE "DH%" THEN REGEXP_EXTRACT(product_name, r'\(([^)]+)\)')
                WHEN product_name LIKE "HC%" THEN REGEXP_EXTRACT(product_name, r'\(([^)]+)\)')
              ELSE
              cp_in_charge
            END
              AS cp_code,
              prod.code,
              CASE
                WHEN prod.commission > 0 THEN (prod.commission*number_units)/shared_bottle
              ELSE
              NULL
            END
              AS sale_commission,
              prod.commission AS raw_commission,
              table_name
            FROM
              `{DATASET}.{TABLE_3}` AS sh
            LEFT JOIN (
              SELECT
                CAST(product_code AS STRING) AS product_code,
                nomenclatore AS code,
                CAST(back AS INT64) AS commission,
                cast(year_month as string) as YEAR_MONTH
              FROM
                `{DATASET}.{PRODUCT_3}` ) AS prod
            ON
              sh.order_code = prod.product_code and sh.business_day like CONCAT(prod.YEAR_MONTH,'%')
            LEFT JOIN (
              SELECT
                DISTINCT CAST(order_number AS STRING) AS order_number,
                table_name
              FROM
                `{DATASET}.{TABLE_4}` ) AS gd
            ON
              sh.order_number = gd.order_number )
          SELECT
            business_day,

            CASE
              WHEN cp_in_charge IS NULL THEN SPLIT(cp_code,",")
            ELSE
            SPLIT(cp_in_charge,",")
          END
            AS cp_final,
            code,
            number_units,
            shared_bottle,
            sale_commission,
            raw_commission,
            table_name
          FROM
            sho_proc
          WHERE
            raw_commission > 0 )
        SELECT
          business_day,
          flat_names,
          CASE
            WHEN LOWER(table_name) LIKE '%t%' THEN 'T'
          ELSE
          'NOT_T'
        END
          AS table_type,
          code,
          CASE WHEN flat_names = '店' THEN 0
          ELSE SUM(sale_commission)
          END AS total
        FROM
          sho_filtered
        CROSS JOIN
          UNNEST(sho_filtered.cp_final) AS flat_names
        GROUP BY
          business_day,
          flat_names,
          table_name,
          code )
      SELECT
        *
      FROM
        sh_piv PIVOT ( SUM(sh_piv.total) AS comission FOR code IN ( 'FD','BT','DR1','DR2','DR3','TP','KA','DH','HC','MR','HR' )) )
      ----------------END PART TWO----------------
      AS part_two
    ON
      CAST(part_one.DAY AS STRING) = CAST(part_two.business_day AS STRING)
      AND part_two.flat_names = part_one.hostess_name )
  ----- PROBLEMATIC PART -----

  SELECT
  * except(
  DAY_1,
  hostess_name_1,
  cp_code_1,
  DAY_t,
  hostess_name_2,
  cp_code_2
  )

  FROM (
  SELECT
    DISTINCT
    DAY,
    hostess_name,
    cp_code,
    working_days,
    working_hours,
    start_time,
    leave_time,
    start_time_p,
    leave_time_p,
    work_time_calculated,
    start_datetime,
    leave_datetime,
    hour_a,
    hour_b,
    a_wage,
    b_wage,
    wage_total_daily
  FROM todo) as t_0
  LEFT JOIN (
          SELECT
        DAY as DAY_1,
        hostess_name as hostess_name_1,
        cp_code as cp_code_1,
        comission_BT AS BT,
        comission_DR1 AS DR1,
        comission_DR2 AS DR2,
        comission_DR3 AS DR3,
        comission_TP AS TP,
        comission_DH AS DH,
        comission_HC AS HC,
        comission_MR AS MR,
        comission_HR AS HR,
      FROM
        todo
      WHERE
        table_type <> 'T' or table_type is null
  ) as t_1
  ON t_0.DAY = t_1.DAY_1 AND t_0.hostess_name = t_1.hostess_name_1 AND t_0.cp_code = t_1.cp_code_1
  LEFT JOIN

      (SELECT
        DAY AS DAY_t,
        hostess_name AS hostess_name_2,
        cp_code AS cp_code_2,
        comission_FD AS FD_T,
        comission_BT AS BT_T,
        comission_DR1 AS DR1_T,
        comission_DR2 AS DR2_T,
        comission_DR3 AS DR3_T,
        comission_TP AS TP_T,
        comission_KA AS KA_T,

        NULL AS total_t
      FROM
        todo
      WHERE
        table_type = 'T' ) AS not_t
    ON
      not_t.DAY_t = t_0.DAY
      AND t_0.hostess_name = not_t.hostess_name_2
      AND not_t.cp_code_2 = t_0.cp_code
  ----- END PROBLEMATIC PART -----
  WHERE
    DAY IS NOT NULL )

---- FINAL SELECT WHERE WE SHOW THE IMPORTANT COLUMNS

SELECT
  CASE
    WHEN total_s.DAY IS NULL THEN ex_t.DAY
  ELSE
  total_s.DAY
END
  AS DAY,
  CASE
    WHEN hostess_name IS NULL THEN ex_t.NAME
  ELSE
  hostess_name
END
  AS hostess_name,
  CASE
    WHEN cp_code IS NULL THEN ex_t.CODE
  ELSE
  cp_code
END
  AS cp_code,
  start_time_p AS starting_time,
  leave_time_p AS leaving_time,
  work_time_calculated AS worked_hours,
  start_datetime,
  leave_datetime,
  wage_total_daily,
  BT,
  DR1,
  DR2,
  DR3,
  ex_t.tip_ret,
  TP,
  DH,
  HC,
  MR,
  HR,
  ex_t.x_20 AS X,
  ex_t.okuri AS `送り`,
  -- CAST(IFNULL(BT,0) + IFNULL(DR1,0) + IFNULL(DR2,0) + IFNULL(DR3,0) + IFNULL(TP,0) + IFNULL(DH,0) + IFNULL(HC,0) + IFNULL(MR,0) + IFNULL(HR,0)+ IFNULL(ex_t.x_20,0)+ IFNULL(ex_t.okuri,0) + IFNULL(wage_total_daily,0) AS INT64) AS TOTAL_NOT_T, --- With wage
  -- CAST(IFNULL(BT,0) + IFNULL(DR1,0) + IFNULL(DR2,0) + IFNULL(DR3,0) + IFNULL(TP,0) + IFNULL(DH,0) + IFNULL(HC,0) + IFNULL(MR,0) + IFNULL(HR,0)+ IFNULL(ex_t.x_20,0)+ IFNULL(ex_t.okuri,0)  AS INT64) AS TOTAL_NOT_T, --- Without wage
  CAST(IFNULL(BT,0) + IFNULL(DR1,0) + IFNULL(DR2,0) + IFNULL(DR3,0) + IFNULL(TP,0) + IFNULL(DH,0) + IFNULL(HC,0) + IFNULL(MR,0) + IFNULL(HR,0)+ IFNULL(ex_t.x_20,0) AS INT64) AS TOTAL_NOT_T, --- Without okuri
  FD_T,
  BT_T,
  DR1_T,
  DR2_T,
  DR3_T,
  TP_T,
  KA_T,
  ex_t.x_395 AS X395,
  (IFNULL(BT_T,0) + IFNULL(DR1_T,0) + IFNULL(DR2_T,0) + IFNULL(DR3_T,0) + IFNULL(TP_T,0) + IFNULL(KA_T,0)  + IFNULL(ex_t.x_395,0)) AS TOTAL_T,
  ex_t.disc as DISC,
  ex_t.adv as ADV,
  ex_t.notes
FROM
  total_s
FULL JOIN
  `{DATASET}.{EXTRA_H}` AS ex_t
ON
  total_s.DAY = ex_t.DAY
  AND cp_code = CODE

------- FILTER AND ORDER -------
)
SELECT
DAY,
hostess_name,
cp_code,
starting_time,
leaving_time,
worked_hours,
start_datetime,
leave_datetime,
wage_total_daily,
BT,
DR1,
DR2,
DR3,
CASE WHEN amount_of_tips = 0 then IFNULL(TP,0)
ELSE tip_ret*250 + IFNULL(TP,0)
END as TP,
DH,
HC,
MR,
HR,
X,
CASE WHEN amount_of_tips = 0 then IFNULL(TOTAL_NOT_T,0)
ELSE IFNULL(tip_ret*250,0) + IFNULL(TOTAL_NOT_T,0)
END as TOTAL_NOT_T,

-- FD_T,
BT_T,
DR1_T,
DR2_T,
DR3_T,
TP_T,
KA_T,
X395,
TOTAL_T,
CASE WHEN amount_of_tips = 0 THEN IFNULL(TOTAL_NOT_T,0) + IFNULL(TOTAL_T,0)
ELSE IFNULL(tip_ret*(250),0) + IFNULL(TOTAL_NOT_T,0) + IFNULL(TOTAL_T,0)
END AS TOTAL_DAY,
`送り`,
DISC,
ADV,
tip_ret as amount_tips,
notes,

from tips_div
LEFT JOIN (
  SELECT * FROM
  (SELECT
    DAY as mise_day,
    sum(TP) as tips_mise,
  from tips_div where hostess_name = '店' group by DAY) as mise
  left join
  (SELECT
    DAY as day_tip,
    sum(tip_ret) as amount_of_tips,
  from tips_div  group by DAY) as tips_amount on mise.mise_day = tips_amount.day_tip
  ) as tips_mis
on tips_mis.day_tip = tips_div.DAY
WHERE tips_div.DAY like "{year}{month_str}%"
ORDER BY
  CAST(DAY AS INT64) ASC,
  cp_code asc
"""
    return query
