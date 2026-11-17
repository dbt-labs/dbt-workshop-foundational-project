-- monthly_sales should have exactly one row per order_month + market_segment.
-- Returns any duplicated combinations; the test fails if any rows come back.

select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by 1, 2
having count(*) > 1
