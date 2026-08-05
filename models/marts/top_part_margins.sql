{{
    config(
        materialized='table'
    )
}}

with parts as (

    select * from {{ ref('stg_tpch__part') }}

),

part_suppliers as (

    select * from {{ ref('stg_tpch__partsupp') }}

),

part_supplier_margins as (

    select
        parts.p_partkey,
        parts.p_name,
        parts.p_mfgr,
        parts.p_brand,
        parts.p_type,
        parts.p_size,
        parts.p_container,
        parts.p_retailprice,
        part_suppliers.ps_suppkey,
        part_suppliers.ps_supplycost,
        part_suppliers.ps_availqty,
        parts.p_retailprice - part_suppliers.ps_supplycost as unit_margin,
        (parts.p_retailprice - part_suppliers.ps_supplycost) * part_suppliers.ps_availqty as supplier_total_margin
    from parts
    inner join part_suppliers
        on parts.p_partkey = part_suppliers.ps_partkey

),

part_margins as (

    select
        p_partkey,
        p_name,
        p_mfgr,
        p_brand,
        p_type,
        p_size,
        p_container,
        p_retailprice,
        max(unit_margin) as best_unit_margin,
        sum(supplier_total_margin) as total_margin
    from part_supplier_margins
    group by 1, 2, 3, 4, 5, 6, 7, 8

),

ranked_parts as (

    select
        *,
        rank() over (order by total_margin desc) as total_margin_rank
    from part_margins

)

select *
from ranked_parts
where total_margin_rank <= 50
