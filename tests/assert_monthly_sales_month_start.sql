-- Monthly buckets must be truncated to the start of the calendar month.
-- Null months are covered by the column's not_null test.
select
    order_month,
    market_segment
from {{ ref('monthly_sales') }}
where order_month <> date_trunc('month', order_month)
