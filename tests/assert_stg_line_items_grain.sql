-- Each order line must be unique within its parent order.
-- If this fails, inspect duplicate source l_orderkey/l_linenumber pairs.
select
    order_key,
    line_number,
    count(*) as row_count
from {{ ref('stg_line_items') }}
group by order_key, line_number
having count(*) > 1
