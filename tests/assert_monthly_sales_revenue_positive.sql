-- total_revenue is gross item sales (pre-discount, pre-tax), so every month/segment
-- bucket should be strictly positive. Zero or negative values point to bad
-- l_extendedprice data in stg_line_items or a broken join in fct_orders.

select
    order_month,
    market_segment,
    total_revenue
from {{ ref('monthly_sales') }}
where total_revenue <= 0
