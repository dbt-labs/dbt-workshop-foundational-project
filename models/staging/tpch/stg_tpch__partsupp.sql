with partsupp as (

    select * from {{ source('tpch', 'partsupp') }}

),

part as (

    select * from {{ ref('stg_tpch__part') }}

),

final as (

    select
        partsupp.ps_partkey,
        partsupp.ps_suppkey,
        partsupp.ps_availqty,
        partsupp.ps_supplycost,
        partsupp.ps_comment,
        part.p_name,
        part.p_mfgr,
        part.p_brand,
        part.p_type,
        part.p_size,
        part.p_container,
        part.p_retailprice,
        part.p_comment as part_comment

    from partsupp
    left join part
        on partsupp.ps_partkey = part.p_partkey

)

select * from final
