select 
    date_trunc('month', order_date) as order_month,
    market_segment,
    count(*) as urgent_order_count
from 
    {{ ref('fct_orders') }}
where 
    priority_code = '1-URGENT'
group by 
    order_month,
    market_segment
order by 
    order_month