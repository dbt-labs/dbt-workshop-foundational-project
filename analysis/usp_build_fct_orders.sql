-- Legacy SQL Server procedure replacement for dbt/Snowflake.
--
-- In this project, `fct_orders` is built as a dbt model in `models/marts/fct_orders.sql`.
-- This analysis reproduces the same result set as a query for inspection and comparison.

with orders as (
    select * from {{ ref('stg_orders') }}
),

line_items as (
    select * from {{ ref('stg_line_items') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

order_item_summary as (
    select
        order_key,
        sum(gross_item_sales_amount) as gross_item_sales_amount,
        sum(item_discount_amount) as item_discount_amount,
        sum(item_tax_amount) as item_tax_amount,
        sum(net_item_sales_amount) as net_item_sales_amount
    from line_items
    group by 1
),

final as (
    select
        orders.order_key,
        orders.order_date,
        orders.customer_key,
        orders.status_code,
        orders.priority_code,
        orders.ship_priority,
        orders.clerk_name,
        customers.name,
        customers.market_segment,
        order_item_summary.gross_item_sales_amount,
        order_item_summary.item_discount_amount,
        order_item_summary.item_tax_amount,
        order_item_summary.net_item_sales_amount
    from orders
    inner join order_item_summary
        on orders.order_key = order_item_summary.order_key
    inner join customers
        on orders.customer_key = customers.customer_key
)

select *
from final
order by order_date
