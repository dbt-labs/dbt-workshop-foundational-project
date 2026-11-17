-- Each calendar month and customer market segment must have a single row.
-- If this fails, inspect monthly_sales for changes to its grouping columns.
select
    order_month,
    market_segment,
    count(*) as row_count
from {{ ref('monthly_sales') }}
group by
    order_month,
    market_segment
having count(*) > 1
