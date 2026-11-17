with 

source as (

    select * from {{ source('tpch', 'customer') }}

),

renamed as (

    select

    from source

)

select * from renamed