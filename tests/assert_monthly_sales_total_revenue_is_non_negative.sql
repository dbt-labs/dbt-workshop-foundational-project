select *
from {{ ref('monthly_sales') }}
where total_revenue < 0
