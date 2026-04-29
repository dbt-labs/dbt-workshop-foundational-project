{{
    config(
        severity = 'warn',
        error_if = '>100'
    )
}}

select gross_item_sales_amount
from {{ ref('fct_orders') }} 
where gross_item_sales_amount < 0