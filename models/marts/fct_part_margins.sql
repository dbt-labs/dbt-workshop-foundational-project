{{
    config(
        materialized='table'
    )
}}

with parts as (

    select * from {{ ref('stg_tpch__parts') }}

),

part_supplier_costs as (

    select * from {{ ref('stg_tpch__partsupp') }}

),

part_margins as (

    select
        parts.part_key,
        parts.part_name,
        parts.manufacturer,
        parts.brand,
        parts.part_type,
        parts.unit_retail_price,
        max(parts.unit_retail_price - part_supplier_costs.ps_supplycost) as best_unit_margin,
        sum(
            (parts.unit_retail_price - part_supplier_costs.ps_supplycost)
            * part_supplier_costs.ps_availqty
        ) as total_margin
    from parts
    inner join part_supplier_costs
        on parts.part_key = part_supplier_costs.ps_partkey
    group by
        parts.part_key,
        parts.part_name,
        parts.manufacturer,
        parts.brand,
        parts.part_type,
        parts.unit_retail_price

),

ranked_parts as (

    select
        *,
        row_number() over (
            order by total_margin desc, part_key
        ) as total_margin_rank
    from part_margins

)

select *
from ranked_parts
where total_margin_rank <= 50
order by total_margin_rank
