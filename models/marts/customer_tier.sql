{{ config(materialized='table') }}

with customers as (

    select customer_key
    from {{ ref('stg_customers') }}

),

orders as (

    select customer_key, total_price
    from {{ ref('stg_orders') }}

),

customer_lifetime_values as (

    select
        customers.customer_key,
        sum(orders.total_price) as lifetime_value
    from customers
    inner join orders
        on customers.customer_key = orders.customer_key
    group by customers.customer_key

)

select
    customer_key::number(38, 0) as customer_key,
    lifetime_value::number(38, 2) as lifetime_value,
    -- Preserve the procedure's CASE order, overlaps, and NULL-producing gaps.
    -- Evaluate the original sum before rounding it to the output precision.
    case
        when lifetime_value <= 200000 then 'tier1'
        when lifetime_value > 2000000 then 'tier2'
        when lifetime_value between 1000000 and 1999999 then 'tier3'
        when lifetime_value between 0 and 999999 then 'tier4'
        else null
    end as tier_name
from customer_lifetime_values
