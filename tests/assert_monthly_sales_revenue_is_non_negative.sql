-- Gross revenue should never be negative.
-- Returns any offending rows; the test fails if any rows come back.

select
    order_month,
    market_segment,
    total_revenue
from {{ ref('monthly_sales') }}
where total_revenue < 0
