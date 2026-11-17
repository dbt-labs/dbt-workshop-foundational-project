select
    order_month,
    market_segment
from {{ ref('monthly_sales') }}
where order_month <> date_trunc('month', order_month)
