--monthly_sales.sql
SELECT
   DATE_TRUNC('month', order_date) AS order_month,
   market_segment,
   SUM(gross_item_sales_amount) AS total_revenue
FROM
   {{ ref('fct_orders') }}
GROUP BY
   1, 2
ORDER BY