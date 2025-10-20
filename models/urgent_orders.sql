select * from {{ref('fct_orders')}}
where priority_code = '1-URGENT'
ORDER BY order_date ASC, 
ORDER_KEY ASC