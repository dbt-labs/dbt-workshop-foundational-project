{{
  config(
    materialized='table'
  )
}}

with orders as (

    select *
    from {{ ref('fct_orders') }}

),

monthly_rollup as (

    select
        date_trunc('month', order_date) as statement_month,
        customer_key,
        name as customer_name,
        market_segment,
        count(*) as order_count,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        sum(gross_item_sales_amount) as gross_item_sales_amount,
        sum(item_discount_amount) as item_discount_amount,
        sum(item_tax_amount) as item_tax_amount,
        sum(net_item_sales_amount) as net_item_sales_amount,
        avg(net_item_sales_amount) as avg_order_net_item_sales_amount
    from orders
    group by 1, 2, 3, 4

),

final as (

    select
        statement_month,
        customer_key,
        customer_name,
        market_segment,
        order_count,
        first_order_date,
        last_order_date,
        gross_item_sales_amount,
        item_discount_amount,
        item_tax_amount,
        net_item_sales_amount,
        avg_order_net_item_sales_amount
    from monthly_rollup

)

select *
from final
order by statement_month, customer_key
