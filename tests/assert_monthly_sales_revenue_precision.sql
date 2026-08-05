select
    order_month,
    market_segment,
    total_revenue
from {{ ref('monthly_sales') }}
where total_revenue != round(total_revenue, 4)
