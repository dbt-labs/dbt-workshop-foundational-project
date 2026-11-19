with source as (

    select * from {{ source('tpch', 'tbl_test') }}

),

renamed as (

    select
        id
    from source

)

select * from renamed
