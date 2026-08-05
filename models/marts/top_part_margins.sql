with parts as (

    select * from {{ ref('stg_tpch__part') }}

),

part_supplier_inventory as (

    select * from {{ ref('stg_tpch__partsupp') }}

),

part_margin_summary as (

    select
        parts.p_partkey as part_key,
        parts.p_name as part_name,
        parts.p_mfgr as manufacturer,
        parts.p_brand as brand,
        parts.p_type as part_type,
        parts.p_retailprice as unit_retail_price,
        sum(part_supplier_inventory.ps_availqty) as total_available_quantity,
        sum(
            part_supplier_inventory.ps_supplycost
            * part_supplier_inventory.ps_availqty
        ) / nullif(sum(part_supplier_inventory.ps_availqty), 0) as weighted_average_unit_cost
    from parts
    inner join part_supplier_inventory
        on parts.p_partkey = part_supplier_inventory.ps_partkey
    group by 1, 2, 3, 4, 5, 6

),

calculated_margins as (

    select
        *,
        unit_retail_price - weighted_average_unit_cost as unit_margin,
        (unit_retail_price - weighted_average_unit_cost)
            * total_available_quantity as total_margin
    from part_margin_summary

),

ranked_parts as (

    select
        *,
        row_number() over (
            order by total_margin desc, unit_margin desc, part_key
        ) as total_margin_rank
    from calculated_margins

)

select *
from ranked_parts
where total_margin_rank <= 50
