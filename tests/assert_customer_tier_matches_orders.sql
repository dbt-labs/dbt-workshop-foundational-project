with legacy_lifetime_value as (

    select
        customers.customer_key,
        sum(orders.total_price) as lifetime_value
    from {{ ref('stg_customers') }} as customers
    inner join {{ ref('stg_orders') }} as orders
        on customers.customer_key = orders.customer_key
    group by customers.customer_key

),

legacy_output as (

    select
        cast(customer_key as numeric(38, 0)) as customer_key,
        cast(lifetime_value as numeric(38, 2)) as lifetime_value,
        case
            when lifetime_value <= 200000 then 'tier1'
            when lifetime_value > 2000000 then 'tier2'
            when lifetime_value between 1000000 and 1999999 then 'tier3'
            when lifetime_value between 0 and 999999 then 'tier4'
        end as tier_name
    from legacy_lifetime_value

),

actual as (

    select customer_key, lifetime_value, tier_name
    from {{ ref('customer_tier') }}

)

select
    coalesce(legacy.customer_key, actual.customer_key) as customer_key,
    legacy.lifetime_value as expected_lifetime_value,
    actual.lifetime_value as actual_lifetime_value,
    legacy.tier_name as expected_tier_name,
    actual.tier_name as actual_tier_name
from legacy_output as legacy
full outer join actual
    on legacy.customer_key = actual.customer_key
where legacy.customer_key is null
    or actual.customer_key is null
    or legacy.lifetime_value is distinct from actual.lifetime_value
    or legacy.tier_name is distinct from actual.tier_name
