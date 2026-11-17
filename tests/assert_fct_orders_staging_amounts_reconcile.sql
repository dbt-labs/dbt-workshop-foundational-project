-- Compare every fact order's amounts with independently aggregated staged items.
-- Null-safe comparisons catch missing amounts without treating null as zero.
-- On failure, inspect the returned expected/actual amounts and staged order items.
with staged_amounts as (

    select
        order_key,
        sum(gross_item_sales_amount) as gross_item_sales_amount,
        sum(item_discount_amount) as item_discount_amount,
        sum(item_tax_amount) as item_tax_amount,
        sum(net_item_sales_amount) as net_item_sales_amount
    from {{ ref('stg_line_items') }}
    group by order_key

)

select
    orders.order_key,
    expected.gross_item_sales_amount as expected_gross_sales_amount,
    orders.gross_item_sales_amount as actual_gross_sales_amount,
    expected.item_discount_amount as expected_discount_amount,
    orders.item_discount_amount as actual_discount_amount,
    expected.item_tax_amount as expected_tax_amount,
    orders.item_tax_amount as actual_tax_amount,
    expected.net_item_sales_amount as expected_net_sales_amount,
    orders.net_item_sales_amount as actual_net_sales_amount
from {{ ref('fct_orders') }} as orders
left join staged_amounts as expected
    on orders.order_key = expected.order_key
where
    expected.order_key is null
    or orders.gross_item_sales_amount is distinct from expected.gross_item_sales_amount
    or orders.item_discount_amount is distinct from expected.item_discount_amount
    or orders.item_tax_amount is distinct from expected.item_tax_amount
    or orders.net_item_sales_amount is distinct from expected.net_item_sales_amount
