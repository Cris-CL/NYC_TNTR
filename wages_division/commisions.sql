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

from `tantra.shosai_2` as sh
---------------------PRODUCTS CODE---------------------
LEFT JOIN (
  SELECT
  product_code,
  CODE as code,
  CAST(back as INT64) as commission
  FROM `test-bigquery-cc.tantra.P2_TABLE`
  UNION ALL

  SELECT
  set_code as product_code,
  CODE as code,
  CAST(REF_1 as INT64) as commission
  FROM `test-bigquery-cc.tantra.P2_SET`
) as prod on sh.order_code = prod.product_code
LEFT JOIN (
  SELECT distinct
  CAST(order_number as STRING) as order_number,
  table_name
  from `tantra.goukei data_2` )
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
  FOR code in ('DR1','DR2','DR3','BT','FD','KA','OT','TB','TP','EN','EX','CR','DH','HC'))
order by business_day asc, flat_names asc
