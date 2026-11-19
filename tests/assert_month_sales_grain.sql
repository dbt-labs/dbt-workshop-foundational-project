-- Monthly reporting must have at most one row per month and market segment.
-- GROUP BY also checks duplicate groups with a null market segment.
select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('month_sales') }}
group by order_month, market_segment
having count(*) > 1
