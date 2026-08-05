{{
    config(
        materialized='table'
    )
}}

with partsupp as (

    select * from {{ ref('stg_tpch__partsupp') }}

),

part as (

    select * from {{ ref('stg_tpch__part') }}

),

product_margins as (

    select
        part.p_partkey,
        partsupp.ps_suppkey,
        part.p_name,
        part.p_mfgr,
        part.p_brand,
        part.p_type,
        part.p_size,
        part.p_container,
        part.p_retailprice,
        partsupp.ps_availqty,
        partsupp.ps_supplycost,
        part.p_retailprice - partsupp.ps_supplycost as unit_margin,
        (part.p_retailprice - partsupp.ps_supplycost) * partsupp.ps_availqty as total_margin
    from partsupp
    inner join part
        on partsupp.ps_partkey = part.p_partkey

)

select *
from product_margins
qualify row_number() over (
    order by total_margin desc, p_partkey, ps_suppkey
) <= 50
order by total_margin desc, p_partkey, ps_suppkey
