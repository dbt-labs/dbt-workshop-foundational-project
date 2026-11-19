-- Reporting grain is one row per calendar month and customer segment.
select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by order_month, market_segment
having count(*) > 1
