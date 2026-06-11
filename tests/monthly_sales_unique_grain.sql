select
  order_month,
  market_segment,
  count(*) as row_count
from {{ ref('monthly_sales') }}
group by 1, 2
having count(*) > 1
