-- Grain check: monthly_sales must have exactly one row per order_month + market_segment.
-- Duplicates usually mean the GROUP BY drifted or a non-grain column was added to it.

select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by 1, 2
having count(*) > 1
