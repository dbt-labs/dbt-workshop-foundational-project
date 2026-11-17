-- Each staged order with matching line items and a customer must appear exactly
-- once in fct_orders. Orders excluded by the current inner joins are not expected.
-- Failed rows identify missing, unexpected, or duplicated fact orders.
with eligible_orders as (

    select distinct orders.order_key
    from {{ ref('stg_orders') }} as orders
    where exists (
        select 1
        from {{ ref('stg_line_items') }} as line_items
        where line_items.order_key = orders.order_key
    )
    and exists (
        select 1
        from {{ ref('stg_customers') }} as customers
        where customers.customer_key = orders.customer_key
    )

),

actual_orders as (

    select
        order_key,
        count(*) as fact_row_count
    from {{ ref('fct_orders') }}
    group by order_key

)

select
    coalesce(expected.order_key, actual.order_key) as order_key,
    actual.fact_row_count,
    case
        when expected.order_key is null then 'unexpected_order'
        when actual.order_key is null then 'missing_order'
        else 'duplicate_order'
    end as failure_reason
from eligible_orders as expected
full outer join actual_orders as actual
    on expected.order_key = actual.order_key
where
    expected.order_key is null
    or actual.order_key is null
    or actual.fact_row_count <> 1
