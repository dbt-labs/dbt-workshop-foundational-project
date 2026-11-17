-- Reconciliation: total revenue in monthly_sales must equal total
-- gross_item_sales_amount in fct_orders. A mismatch means rows were dropped
-- (e.g. null order_date or market_segment) or double counted during aggregation.

with mart as (
    select sum(total_revenue) as total_revenue
    from {{ ref('monthly_sales') }}
),

fct as (
    select sum(gross_item_sales_amount) as total_revenue
    from {{ ref('fct_orders') }}
)

select
    mart.total_revenue as mart_total_revenue,
    fct.total_revenue as fct_total_revenue
from mart
cross join fct
where abs(coalesce(mart.total_revenue, 0) - coalesce(fct.total_revenue, 0)) > 0.01
