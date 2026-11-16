{{ config(materialized='table') }}

with customer_lifetime_value as (

    select
        customers.customer_key,
        sum(orders.total_price) as lifetime_value
    from {{ ref('stg_customers') }} as customers
    inner join {{ ref('stg_orders') }} as orders
        on customers.customer_key = orders.customer_key
    group by customers.customer_key

)

select
    customer_key::number(38, 0) as customer_key,
    lifetime_value::number(38, 2) as lifetime_value,
    -- Preserve the procedure's CASE order and gaps. Classify the unrounded sum.
    case
        when lifetime_value <= 200000 then 'tier1'
        when lifetime_value > 2000000 then 'tier2'
        when lifetime_value between 1000000 and 1999999 then 'tier3'
        when lifetime_value between 0 and 999999 then 'tier4'
        else null
    end::varchar as tier_name
from customer_lifetime_value
