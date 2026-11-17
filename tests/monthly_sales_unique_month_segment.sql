-- Grain: one row per month and market segment.
-- If this fails, inspect monthly_sales for joins or grouping changes
-- that introduce duplicate month-segment pairs.
select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by
    order_month,
    market_segment
having count(*) > 1
