-- Reconcile all output rows against the original procedure's SELECT logic.
-- NULL-safe comparisons preserve intentional NULL lifetime values and tiers.
with original_aggregation as (

    select
        customers.customer_key,
        sum(orders.total_price) as lifetime_value
    from {{ ref('stg_customers') }} as customers
    inner join {{ ref('stg_orders') }} as orders
        on customers.customer_key = orders.customer_key
    group by customers.customer_key

),

expected as (

    select
        customer_key::number(38, 0) as customer_key,
        lifetime_value::number(38, 2) as lifetime_value,
        case
            when lifetime_value <= 200000 then 'tier1'
            when lifetime_value > 2000000 then 'tier2'
            when lifetime_value between 1000000 and 1999999 then 'tier3'
            when lifetime_value between 0 and 999999 then 'tier4'
            else null
        end as tier_name
    from original_aggregation

),

actual as (

    select customer_key, lifetime_value, tier_name
    from {{ ref('customer_tier') }}

)

select
    coalesce(expected.customer_key, actual.customer_key) as customer_key,
    expected.lifetime_value as expected_lifetime_value,
    actual.lifetime_value as actual_lifetime_value,
    expected.tier_name as expected_tier_name,
    actual.tier_name as actual_tier_name
from expected
full outer join actual
    on expected.customer_key = actual.customer_key
where expected.customer_key is null
    or actual.customer_key is null
    or expected.lifetime_value is distinct from actual.lifetime_value
    or expected.tier_name is distinct from actual.tier_name
