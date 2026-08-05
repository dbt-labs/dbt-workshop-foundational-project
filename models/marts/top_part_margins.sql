with parts as (

    select * from {{ ref('stg_tpch__part') }}

),

part_supplier_costs as (

    select
        ps_partkey,
        sum(ps_availqty) as total_available_quantity,
        sum(ps_availqty * ps_supplycost) / nullif(sum(ps_availqty), 0) as weighted_avg_supply_cost
    from {{ ref('stg_tpch__partsupp') }}
    group by 1

),

part_margins as (

    select
        p.p_partkey,
        p.p_name,
        p.p_mfgr,
        p.p_brand,
        p.p_type,
        p.p_size,
        p.p_container,
        p.p_retailprice,
        ps.total_available_quantity,
        ps.weighted_avg_supply_cost,
        p.p_retailprice - ps.weighted_avg_supply_cost as unit_margin,
        (p.p_retailprice - ps.weighted_avg_supply_cost) * ps.total_available_quantity as total_margin
    from parts as p
    inner join part_supplier_costs as ps
        on p.p_partkey = ps.ps_partkey

),

ranked_parts as (

    select
        *,
        row_number() over (
            order by unit_margin desc, p_partkey
        ) as unit_margin_rank,
        row_number() over (
            order by total_margin desc, p_partkey
        ) as total_margin_rank
    from part_margins

)

select *
from ranked_parts
where total_margin_rank <= 50
