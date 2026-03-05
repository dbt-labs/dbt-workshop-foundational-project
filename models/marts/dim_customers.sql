with customers as (
    select * from {{ ref('stg_customers') }}
),

nations as (
    select * from {{ ref('stg_nation') }}
),


final as (
    select 
    customers.customer_key,
    customers.name,
    customers.address,
    customers.nation_key,
    nations.nation
    from customers
    inner join nations on customers.nation_key = nations.nation_key

)

select * from final