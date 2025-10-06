with customer as (
    select * from {{ref('stg_customers')}}
),
nation as (
    select * from {{ ref('stg_nations') }}
),
region as (
    select * from {{ ref('stg_regions') }}
)

select
    customer.customer_key,
    customer.name,
    customer.address,
    nation.name as nation,
    region.name as nation,
    customer.phone_number,
    customer.account_balance,
    customer.market_segment
from
    customer
    inner join nation
        on customer.nation_key = nation.nation_key
    inner join region
        on nation.nation_key = region.region_key