with parts as (

    select * from {{ ref('stg_tpch__part') }}

),

part_suppliers as (

    select * from {{ ref('stg_tpch__partsupp') }}

),

supplier_margins as (

    select
        parts.p_partkey as part_key,
        parts.p_name as part_name,
        parts.p_mfgr as manufacturer_name,
        parts.p_brand as brand_name,
        parts.p_type as part_type,
        parts.p_retailprice as unit_retail_price,
        part_suppliers.ps_suppkey as supplier_key,
        part_suppliers.ps_supplycost as unit_cost,
        part_suppliers.ps_availqty as available_quantity,
        (parts.p_retailprice - part_suppliers.ps_supplycost)::decimal(16, 4) as unit_margin,
        (
            (parts.p_retailprice - part_suppliers.ps_supplycost)
            * part_suppliers.ps_availqty
        )::decimal(24, 4) as supplier_total_margin
    from parts
    inner join part_suppliers
        on parts.p_partkey = part_suppliers.ps_partkey

),

part_margins as (

    select
        part_key,
        part_name,
        manufacturer_name,
        brand_name,
        part_type,
        unit_retail_price,
        min(unit_cost) as lowest_unit_cost,
        sum(available_quantity) as total_available_quantity,
        max(unit_margin) as best_unit_margin,
        sum(supplier_total_margin)::decimal(24, 4) as total_margin
    from supplier_margins
    group by 1, 2, 3, 4, 5, 6

),

ranked_parts as (

    select
        *,
        row_number() over (
            order by total_margin desc, best_unit_margin desc, part_key asc
        ) as margin_rank
    from part_margins

)

select *
from ranked_parts
where margin_rank <= 50
order by margin_rank
