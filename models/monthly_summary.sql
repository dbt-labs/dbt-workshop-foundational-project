with monthly_summary as (
    select 
        date_trunc('month', order_date) as order_month,
        market_segment,
        sum(gross_item_sales_amount) as total_gross_sales,
        sum(item_discount_amount) as total_discounts,
        sum(item_tax_amount) as total_taxes,
        sum(net_item_sales_amount) as total_net_sales
    from {{ ref('fct_orders') }}
    group by 1, 2
)

select *
from monthly_summary
order by order_month, market_segment