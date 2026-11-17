-- dim_customers inner joins customers to nations and regions. Any customer whose
-- nation_key (or nation's region_key) has no match is silently dropped.
-- This returns every customer in stg_customers that is missing from dim_customers.

select
    stg.customer_key,
    stg.nation_key
from {{ ref('stg_customers') }} as stg
left join {{ ref('dim_customers') }} as dim
    on stg.customer_key = dim.customer_key
where dim.customer_key is null
