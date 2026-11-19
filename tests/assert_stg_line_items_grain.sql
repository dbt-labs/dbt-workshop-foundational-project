-- Each order may contain many lines, but each line number must occur once per order.
-- If this fails, inspect duplicate (l_orderkey, l_linenumber) pairs in TPCH lineitem.
select
    order_key,
    line_number,
    count(*) as row_count
from {{ ref('stg_line_items') }}
group by order_key, line_number
having count(*) > 1
