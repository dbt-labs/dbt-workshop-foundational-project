with validation_errors as (

    select
        order_month,
        order_count,
        customer_count,
        gross_sales_amount,
        discount_amount,
        tax_amount,
        net_sales_amount,
        avg_order_net_sales_amount
    from {{ ref('monthly_sales') }}
    where order_count < 1
       or customer_count < 1
       or customer_count > order_count
       or gross_sales_amount < 0
       or tax_amount < 0
       or net_sales_amount < 0
       or discount_amount > 0
       or abs((gross_sales_amount + discount_amount + tax_amount) - net_sales_amount) > 0.01
       or abs((net_sales_amount / nullif(order_count, 0)) - avg_order_net_sales_amount) > 0.01

)

select *
from validation_errors
