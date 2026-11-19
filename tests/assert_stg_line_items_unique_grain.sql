-- Each source line must occur once within its parent order.
select
    order_key,
    line_number,
    count(*) as row_count
from {{ ref('stg_line_items') }}
group by order_key, line_number
having count(*) > 1
