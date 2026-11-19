-- Each order line must appear once so joins and sales aggregations do not fan out.
select
    order_key,
    line_number,
    count(*) as row_count
from {{ ref('stg_line_items') }}
group by order_key, line_number
having count(*) > 1
