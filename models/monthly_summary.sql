with monthly_revenue as (
    select 
        date_trunc('month', order_date) as order_month,
        market_segment,
        sum(net_item_sales_amount) as total_revenue
    from {{ ref('fct_orders') }}
    group by 1, 2
)

select 
    order_month,
    market_segment,
    total_revenue
from monthly_revenue
order by order_month, market_segment