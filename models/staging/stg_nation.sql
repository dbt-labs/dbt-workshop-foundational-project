with 

source as (

    select * from {{ source('tpch', 'nation') }}

),

renamed as (

    select
        n_nationkey as nation_key,
        n_name as nation,
        n_regionkey as region_key,
        n_comment as nation_comment

    from source

)

select * from renamed