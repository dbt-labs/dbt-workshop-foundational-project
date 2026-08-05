with parts as (

    select
        p_partkey,
        p_name,
        p_mfgr,
        p_brand,
        p_type,
        p_size,
        p_container,
        p_retailprice
    from {{ ref('stg_tpch__part') }}

),

part_suppliers as (

    select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost
    from {{ ref('stg_tpch__partsupp') }}

),

final as (

    select
        parts.p_partkey as part_key,
        parts.p_name as part_name,
        parts.p_mfgr as manufacturer,
        parts.p_brand as brand,
        parts.p_type as part_type,
        parts.p_size as part_size,
        parts.p_container as container,
        part_suppliers.ps_suppkey as supplier_key,
        parts.p_retailprice as unit_retail_price,
        part_suppliers.ps_supplycost as unit_cost,
        part_suppliers.ps_availqty as available_quantity,
        parts.p_retailprice - part_suppliers.ps_supplycost as unit_margin,
        (parts.p_retailprice - part_suppliers.ps_supplycost) * part_suppliers.ps_availqty as total_margin
    from parts
    inner join part_suppliers
        on parts.p_partkey = part_suppliers.ps_partkey

)

select *
from final
