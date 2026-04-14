
select gross_item_sales_amount
from {{ ref('fct_orders') }}
-- from my_db.my_schema.my_table
where gross_item_sales_amount < 0