with source as (

    select * from {{ source('tpch', 'part') }}

),

renamed as (

    select
        p_partkey as part_key,
        p_name as part_name,
        p_retailprice as retail_price

    from source

)

select * from renamed
