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
        parts.part_key,
        parts.part_name,
        part_suppliers.supplier_key,
        part_suppliers.available_quantity,
        (parts.retail_price - part_suppliers.supply_cost) as best_unit_margin

    from parts
    inner join part_suppliers
        on parts.part_key = part_suppliers.part_key

),

best_supplier_per_part as (

    select
        *,
        row_number() over (
            partition by part_key
            order by best_unit_margin desc, supplier_key
        ) as supplier_margin_rank

    from part_supplier_margins

),

ranked_products as (

    select
        part_key,
        part_name,
        supplier_key,
        available_quantity,
        best_unit_margin,
        (best_unit_margin * available_quantity) as total_margin,
        row_number() over (
            order by (best_unit_margin * available_quantity) desc, part_key
        ) as total_margin_rank

    from best_supplier_per_part
    where supplier_margin_rank = 1

)

select *
from ranked_products
where total_margin_rank <= 50
order by total_margin_rank
