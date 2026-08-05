with parts as (

    select * from {{ ref('stg_tpch__part') }}

),

part_supplier_costs as (

    select * from {{ ref('stg_tpch__partsupp') }}

),

part_margins as (

    select
        parts.p_partkey,
        parts.p_name,
        parts.p_mfgr,
        parts.p_brand,
        parts.p_type,
        parts.p_retailprice as unit_retail_price,
        sum(part_supplier_costs.ps_availqty) as total_quantity,
        sum(part_supplier_costs.ps_supplycost * part_supplier_costs.ps_availqty)
            / nullif(sum(part_supplier_costs.ps_availqty), 0) as unit_cost,
        parts.p_retailprice
            - (
                sum(part_supplier_costs.ps_supplycost * part_supplier_costs.ps_availqty)
                / nullif(sum(part_supplier_costs.ps_availqty), 0)
            ) as unit_margin,
        sum(
            (parts.p_retailprice - part_supplier_costs.ps_supplycost)
            * part_supplier_costs.ps_availqty
        ) as total_margin

    from parts
    inner join part_supplier_costs
        on parts.p_partkey = part_supplier_costs.ps_partkey

    group by
        parts.p_partkey,
        parts.p_name,
        parts.p_mfgr,
        parts.p_brand,
        parts.p_type,
        parts.p_retailprice

),

ranked_parts as (

    select
        *,
        row_number() over (
            order by total_margin desc, p_partkey asc
        ) as margin_rank

    from part_margins

)

select *
from ranked_parts
where margin_rank <= 50
