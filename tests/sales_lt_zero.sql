SELECT gross_item_sales_amount
FROM {{ref('fct_orders')}}
where gross_item_sales_amount < 0