select
    order_key,
    line_number,
    count(*) as record_count
from {{ ref('stg_line_items') }}
group by order_key, line_number
having count(*) > 1
