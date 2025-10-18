SELECT 
    DATE_TRUNC('month', order_date) AS order_month,
    COUNT(*) AS urgent_order_count
FROM 
    {{ ref('fct_orders') }}
WHERE 
    priority_code = '1-URGENT'
GROUP BY 
    order_month
ORDER BY 
    order_month