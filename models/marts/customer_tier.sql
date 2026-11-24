{{ config(materialized='table') }}

with customer_lifetime_value as (

    select
        customers.customer_key,
        sum(orders.total_price) as lifetime_value
    from {{ ref('stg_customers') }} as customers
    inner join {{ ref('stg_orders') }} as orders
        on customers.customer_key = orders.customer_key
    group by customers.customer_key

),

tiered as (

    select
        customer_key,
        lifetime_value,
        -- Preserve the legacy CASE order and uncovered ranges during migration.
        case
            when lifetime_value <= 200000 then 'tier1'
            when lifetime_value > 2000000 then 'tier2'
            when lifetime_value between 1000000 and 1999999 then 'tier3'
            when lifetime_value between 0 and 999999 then 'tier4'
        end as tier_name
    from customer_lifetime_value

)

select
    cast(customer_key as numeric(38, 0)) as customer_key,
    cast(lifetime_value as numeric(38, 2)) as lifetime_value,
    cast(tier_name as varchar) as tier_name
from tiered
