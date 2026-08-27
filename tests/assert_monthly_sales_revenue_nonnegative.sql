-- Negative gross revenue indicates an upstream aggregation or source-data issue.
-- Debug by tracing the affected month and segment into fct_orders and stg_line_items.
select
    order_month,
    market_segment,
    total_revenue
from {{ ref('monthly_sales') }}
where total_revenue < 0
