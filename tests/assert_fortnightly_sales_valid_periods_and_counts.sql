-- Return periods with invalid bucket boundaries or impossible order/customer counts.
{% set anchor_date = var('fortnight_anchor_date', '1991-12-30') %}

select *
from {{ ref('fortnightly_sales') }}
where
    datediff('day', fortnight_start_date, fortnight_end_date) <> 13
    or mod(datediff('day', cast('{{ anchor_date }}' as date), fortnight_start_date), 14) <> 0
    or order_count <= 0
    or customer_count < 0
    or customer_count > order_count
