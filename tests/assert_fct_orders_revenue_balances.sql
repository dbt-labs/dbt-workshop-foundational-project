-- Discounts are negative; net sales must reconcile to gross + discount + tax.
-- Allow one unit at the staging model's four-decimal precision for rounding.
select
    order_key,
    gross_item_sales_amount,
    item_discount_amount,
    item_tax_amount,
    net_item_sales_amount,
    net_item_sales_amount
        - (gross_item_sales_amount + item_discount_amount + item_tax_amount)
        as revenue_difference
from {{ ref('fct_orders') }}
where abs(
    net_item_sales_amount
    - (gross_item_sales_amount + item_discount_amount + item_tax_amount)
) > 0.0001
