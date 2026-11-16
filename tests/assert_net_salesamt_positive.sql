-- Singular tests pass when this query returns no rows.
-- Return each order whose total net item sales amount is negative.
select
    order_key,
    sum(net_item_sales_amount) as net_sales_amount
from {{ ref('fct_orders') }}
group by order_key
having sum(net_item_sales_amount) < 0
