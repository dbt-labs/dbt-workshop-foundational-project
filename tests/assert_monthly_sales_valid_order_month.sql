-- order_month must be the first day of a month and must not be in the future.
-- Future months usually indicate bad order_date values in stg_orders.

select
    order_month,
    market_segment
from {{ ref('monthly_sales') }}
where order_month <> date_trunc('month', order_month)
   or order_month > date_trunc('month', current_date())
