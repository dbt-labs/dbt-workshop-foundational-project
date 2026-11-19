{{ config(materialized='table') }}

with customers as (

    select * from {{ ref('stg_customers') }}

),

nations as (

    select * from {{ ref('stg_nations') }}

),

regions as (

    select * from {{ ref('stg_regions') }}

),

final as (

    select
        customers.customer_key,
        customers.name,
        customers.address,
        nations.name as nation_name,
        regions.name as region_name,
        customers.phone_number,
        customers.account_balance,
        customers.market_segment,
        customers.comment
    from customers
    left join nations
        on customers.nation_key = nations.nation_key
    left join regions
        on nations.region_key = regions.region_key

)

select * from final

