select *
from {{ ref('fct_orders') }}
where net_item_sales_amount < 0
