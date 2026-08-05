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

part_costs as (

    select
        ps_partkey as part_key,
        sum(ps_availqty) as total_quantity,
        sum(ps_supplycost * ps_availqty) / nullif(sum(ps_availqty), 0) as unit_cost
    from part_suppliers
    group by 1

),

part_margins as (

    select
        parts.part_key,
        parts.part_name,
        parts.manufacturer_name,
        parts.brand_name,
        parts.part_type,
        parts.part_size,
        parts.container_type,
        parts.unit_retail_price,
        part_costs.unit_cost,
        parts.unit_retail_price - part_costs.unit_cost as unit_margin,
        part_costs.total_quantity,
        (parts.unit_retail_price - part_costs.unit_cost) * part_costs.total_quantity as total_margin
    from parts
    inner join part_costs
        on parts.part_key = part_costs.part_key

),

ranked as (

    select
        *,
        row_number() over (
            order by total_margin desc, unit_margin desc, part_key
        ) as total_margin_rank
    from part_margins

)

select *
from ranked
where total_margin_rank <= 50
order by total_margin_rank
