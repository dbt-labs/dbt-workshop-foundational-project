with

source as (

    select * from {{ source('tpch', 'partsupp') }}

),

renamed as (

    select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment

    from source

),

part as (

    select * from {{ ref('stg_tpch__part') }}

),

joined as (

    select
        renamed.ps_partkey,
        renamed.ps_suppkey,
        renamed.ps_availqty,
        renamed.ps_supplycost,
        renamed.ps_comment,
        part.p_name,
        part.p_mfgr,
        part.p_brand,
        part.p_type,
        part.p_size,
        part.p_container,
        part.p_retailprice,
        part.p_comment as p_comment

    from renamed
    left join part
        on renamed.ps_partkey = part.p_partkey

)

select * from joined
