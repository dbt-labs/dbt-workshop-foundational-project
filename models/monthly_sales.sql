with orders as (

    select
        order_date,
        market_segment,
        gross_item_sales_amount
    from {{ ref('fct_orders') }}

)

select
    date_trunc('month', order_date) as order_month,
    market_segment,
    sum(gross_item_sales_amount) as total_revenue
from orders
group by 1, 2
