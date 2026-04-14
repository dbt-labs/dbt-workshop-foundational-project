{{
    config(
        materialized='table'
    )
}}

with orders as (

    select *
    from {{ ref('fct_orders') }}

),

final as (

    select
        date_trunc('month', order_date) as order_month,
        count(*) as order_count,
        count(distinct customer_key) as customer_count,
        sum(gross_item_sales_amount) as gross_sales_amount,
        sum(item_discount_amount) as discount_amount,
        sum(item_tax_amount) as tax_amount,
        sum(net_item_sales_amount) as net_sales_amount
    from orders
    group by 1

)

select *
from final
order by order_month
