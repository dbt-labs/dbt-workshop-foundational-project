-- Rates use fractions; discounts are represented as negative amounts.
-- Check source rates and quantities before changing the staging calculation.
select
    order_key,
    line_number,
    quantity,
    discount_percentage,
    tax_rate,
    gross_item_sales_amount,
    item_discount_amount
from {{ ref('stg_line_items') }}
where quantity <= 0
    or discount_percentage is null
    or discount_percentage < 0
    or discount_percentage > 1
    or tax_rate is null
    or tax_rate < 0
    or tax_rate > 1
    or gross_item_sales_amount < 0
    or item_discount_amount > 0
