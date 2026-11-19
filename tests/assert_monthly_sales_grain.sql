-- Monthly reporting requires one row per month and customer segment.
-- If this fails, check the grouping columns in monthly_sales.
select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by order_month, market_segment
having count(*) > 1
