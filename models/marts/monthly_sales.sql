{{
  config(
    materialized='table'
  )
}}

with orders as (

    select * from {{ ref('fct_orders') }}

),

final as (

    select
        date_trunc('month', order_date)::date as order_month,
        sum(net_item_sales_amount) as revenue
    from orders
    group by 1

)

select *
from final
order by order_month
