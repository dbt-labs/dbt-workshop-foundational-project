SELECT
    date_trunc('month', order_date) as order_month,
    market_segment,
    SUM(gross_item_sales_amount) as total_revenue
FROM {{ ref('fct_orders') }}
GROUP BY 1,2
ORDER BY 1,2