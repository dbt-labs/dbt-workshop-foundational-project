select gross_item_sales_amount
from {{ref('fct_order')}}
where gross_item_sales_amount < 0
