{{
    config(
        materialized='table'
    )
}}

with customers as (

    select *
    from {{ ref('stg_customers') }}

),

final as (

    select
        customer_key,
        name,
        address,
        nation_key,
        phone_number,
        account_balance,
        market_segment,
        comment
    from customers

)

select *
from final
