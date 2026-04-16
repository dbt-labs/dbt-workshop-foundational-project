select gross_item_sales_amount
from {{ ref('fct_orders')}}
where gross_item_sales_amount < 0