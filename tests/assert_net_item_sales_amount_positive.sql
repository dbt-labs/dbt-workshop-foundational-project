{{
    config(
        severity = "warn",
        error_if = ">100"
    )
}}

select
   order_key,
   sum(net_item_sales_amount) as amount
from {{ ref('fct_orders') }}
group by 1
having not(amount >= 0)