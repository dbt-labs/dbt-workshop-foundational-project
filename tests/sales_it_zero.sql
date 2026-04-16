select *
from {{ ref('monthly_sales') }}
where net_sales_amount = 0
