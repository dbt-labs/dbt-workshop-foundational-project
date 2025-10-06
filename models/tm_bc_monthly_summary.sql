select 
    date_trunc('month', order_date) as order_month,
    market_segment,
    sum(net_item_sales_amount) as total_net_sales
from {{ ref('fct_orders') }}
group by 1, 2
order by 1, 2