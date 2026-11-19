-- The reporting grain is one row per calendar month and customer market segment.
-- If this fails, inspect monthly_sales for extra grouping columns or duplicate joins.
select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by order_month, market_segment
having count(*) > 1
