select
    order_month,
    market_segment
from {{ ref('monthly_sales_leyla') }}
where order_month <> date_trunc('month', order_month)
