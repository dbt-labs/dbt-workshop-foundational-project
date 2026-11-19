-- A line number is unique only within its order. Inspect duplicate key pairs
-- in the source if this test fails; do not silently deduplicate staging rows.
select
    order_key,
    line_number,
    count(*) as row_count
from {{ ref('stg_line_items') }}
group by order_key, line_number
having count(*) > 1
