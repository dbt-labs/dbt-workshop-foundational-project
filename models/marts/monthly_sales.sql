{{
    config(
        materialized='table'
    )
}}

with orders as (

    select * from {{ ref('fct_orders') }}

),

monthly as (

    select
        date_trunc('month', order_date)::date as order_month,
        count(distinct order_key) as order_count,
        sum(gross_item_sales_amount) as gross_revenue,
        sum(item_discount_amount) as discount_amount,
        sum(item_tax_amount) as tax_amount,
        sum(net_item_sales_amount) as net_revenue
    from orders
    group by 1

)

select * from monthly
