-- A valid order month is non-null and aligned to the start of a month.
select
    order_month
from {{ ref('monthly_sales') }}
where order_month is null
    or order_month != date_trunc('month', order_month)
