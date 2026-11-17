-- Discounts are signed negative amounts; net sales includes tax.
select *
from {{ ref('fortnightly_sales') }}
where abs(
    net_sales_amount - (gross_sales_amount + discount_amount + tax_amount)
) > 0.0001
