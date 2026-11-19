with source as (

    select * from {{ source('tpch', 'tbl_test') }}

)

select id from source
