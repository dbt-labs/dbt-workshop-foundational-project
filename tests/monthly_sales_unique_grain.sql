-- Each calendar month and customer market segment must have at most one row.
-- If this fails, inspect changes to the grouping or joins in monthly_sales.
select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by
    order_month,
    market_segment
having count(*) > 1
