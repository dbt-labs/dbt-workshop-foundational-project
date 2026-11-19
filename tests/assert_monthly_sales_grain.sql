-- The reporting grain is one row per month and customer market segment.
select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by order_month, market_segment
having count(*) > 1
