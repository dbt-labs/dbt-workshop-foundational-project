{{
    config(
        tags = ['daily','finance'] 
    )
}}    



with customer as (
    select * from {{ ref('stg_customers')}}
),
nation as (
    select * from {{ ref('stg_nations')}}
),
region as (
    select * from {{ ref('stg_regions')}}
)

select 
     customer.customer_key,
     customer.name,
     customer.address,
     customer.name as nation,
     region.name as region,
     customer.phone_number,
     customer.account_balance,
     customer.market_segment
from    
    customer
    inner join nation
        on customer.nation_key = nation.nation_key
    inner join region
        on nation.region_key = region.region_key






models:
    foundation_project:
     +materialized:view

    staging:
        tags: ['silver']

    marts:
        tags: ['gold']
        + materialized : table