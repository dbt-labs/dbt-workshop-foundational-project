select 
    date_trunc('month', order_date) as order_month,
    count(*) as urgent_order_count
from {{ ref('fct_orders') }}
where priority_code = '1-URGENT'
group by 1
order by 1