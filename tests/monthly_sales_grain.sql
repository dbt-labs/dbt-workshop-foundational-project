select
    order_month,
    market_segment
from {{ ref('monthly_sales') }}
group by 1, 2
having count(*) > 1
