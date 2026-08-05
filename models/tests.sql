with part_supplier_costs as (

    select
        ps_partkey,
        sum(ps_availqty) as total_quantity,
        sum(ps_availqty * ps_supplycost) / nullif(sum(ps_availqty), 0) as unit_cost
    from {{ ref('stg_tpch__partsupp') }}
    group by 1

),

part_margins as (

    select
        part.p_partkey,
        part.p_name as part_name,
        part.p_brand as part_brand,
        part.p_retailprice as unit_retail_price,
        part_supplier_costs.total_quantity,
        part_supplier_costs.unit_cost,
        part.p_retailprice - part_supplier_costs.unit_cost as unit_margin,
        (part.p_retailprice - part_supplier_costs.unit_cost) * part_supplier_costs.total_quantity as total_margin
    from {{ ref('stg_tpch__part') }} as part
    inner join part_supplier_costs
        on part.p_partkey = part_supplier_costs.ps_partkey

),

ranked_parts as (

    select
        *,
        row_number() over (
            order by total_margin desc, p_partkey asc
        ) as margin_rank
    from part_margins

)

select
    p_partkey,
    part_name,
    part_brand,
    unit_retail_price,
    total_quantity,
    unit_cost,
    unit_margin,
    total_margin,
    margin_rank
from ranked_parts
where margin_rank <= 50
order by margin_rank
