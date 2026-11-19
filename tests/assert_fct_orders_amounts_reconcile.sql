-- Discounts are negative and tax is added after discounts.
-- Inspect the contributing stg_line_items records when an order fails.
select
    order_key,
    gross_item_sales_amount,
    item_discount_amount,
    item_tax_amount,
    net_item_sales_amount
from {{ ref('fct_orders') }}
where gross_item_sales_amount is null
    or item_discount_amount is null
    or item_tax_amount is null
    or net_item_sales_amount is null
    or abs(
        net_item_sales_amount
        - (gross_item_sales_amount + item_discount_amount + item_tax_amount)
    ) > 0.0001
