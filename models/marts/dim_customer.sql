with customers as (

    select * from {{ ref('stg_customers') }}

),

nations as (

    select * from {{ ref('stg_nation') }}

),

regions as (

    select * from {{ ref('stg_region') }}

),

final as (

    select
        customers.customer_key,
        customers.name,
        customers.address,
        nations.nation_name as nation,
        regions.region_name as region,
        customers.phone_number,
        customers.account_balance,
        customers.market_segment
    from customers
    inner join nations
        on customers.nation_key = nations.nation_key
    inner join regions
        on nations.region_key = regions.region_key

)

select *
from final
