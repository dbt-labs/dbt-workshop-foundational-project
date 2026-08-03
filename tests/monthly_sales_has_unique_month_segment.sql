-- Return duplicate month and market-segment groups; the monthly_sales mart must have one row per group.
select
    order_month,
    market_segment
from {{ ref('monthly_sales') }}
group by 1, 2
having count(*) > 1
