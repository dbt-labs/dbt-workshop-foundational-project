-- A month and customer segment must identify a single reporting row.
-- Grouping directly also catches duplicate rows with null segments.
select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by order_month, market_segment
having count(*) > 1
