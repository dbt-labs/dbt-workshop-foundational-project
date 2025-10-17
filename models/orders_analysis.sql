
SELECT 
    *,
    DATE_TRUNC('month', order_date) AS order_month
FROM 
    {{ ref('fct_orders') }}
ORDER BY 
    order_month ASC,
    priority_code ASC