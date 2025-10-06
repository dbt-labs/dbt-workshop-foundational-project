with customer as (
    select * from {{ ref('stg_customers')}}
),
    select * from {{ ref('stg_nations')}}
),
    select * from {{ ref('stg_regions')}}
)

select
    customer.customer_key,
    customer.name,
    customer.address,
    nation.name as nation,
    region.name as region,
    customer.phone_numnber,
    customer.market_segment
fromcustomer
inner join nation
    on customer.nation_key = nation.nation_key
inner join region
    on nation.region_key = region.region_key
    