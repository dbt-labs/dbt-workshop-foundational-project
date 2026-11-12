select 
    date_trunc('month', order_date) as order_month,
    market_segment,
    sum(gross_item_sales_amount) as gross_item_sales_amount

from
    {{ ref('fct_orders') }}

group by
    1, 2