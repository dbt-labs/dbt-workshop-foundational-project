-- Each month and customer market segment must have exactly one row.
-- If this fails, inspect changes to the aggregation grain in monthly_sales.
select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by order_month, market_segment
having count(*) > 1
