with source as (

    select * from {{ source('tpch', 'part') }}

),

renamed as (

    select
        p_partkey as part_key,
        p_name as part_name,
        p_mfgr as manufacturer_name,
        p_brand as brand_name,
        p_type as part_type,
        p_size as part_size,
        p_container as container_type,
        p_retailprice as unit_retail_price,
        p_comment as comment
    from source

)

select * from renamed

with 

source as (

    select * from {{ source('tpch', 'part') }}

),

renamed as (

    select
        p_partkey,
        p_name,
        p_mfgr,
        p_brand,
        p_type,
        p_size,
        p_container,
        p_retailprice,
        p_comment

    from source

)

select * from renamed