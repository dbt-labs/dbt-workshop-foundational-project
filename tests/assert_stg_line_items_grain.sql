-- A line number identifies a line only within its parent order.
-- Investigate source duplicates or changes to staging grain if this fails.
select
    order_key,
    line_number,
    count(*) as row_count
from {{ ref('stg_line_items') }}
group by order_key, line_number
having count(*) > 1
