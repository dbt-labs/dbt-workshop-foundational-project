with customer as (
    select * from {{ ref('stg_customers') }}
),

nation as (
    select * from {{ ref('stg_nation') }}
),

region as (
    select * from {{ ref('stg_region') }}
)

select 
    customer.customer_key, 
    customer.name as customer_name, 
    customer.address, 
    nation.nation, 
    region.region,
    customer.phone_number, 
    customer.account_balance, 
    customer.market_segment
from 
    customer 
    inner join nation 
        on customer.nation_key = nation.nation_key
    inner join region 
        on nation.region_key = region.region_key