with parts as (

    select * from {{ ref('stg_tpch__part') }}

),

part_supply as (

    select * from {{ ref('stg_tpch__partsupp') }}

),

part_margins as (

    select
        parts.p_partkey as part_key,
        parts.p_name as part_name,
        parts.p_mfgr as manufacturer,
        parts.p_brand as brand,
        parts.p_type as part_type,
        parts.p_retailprice as unit_retail_price,
        sum(part_supply.ps_availqty) as total_available_quantity,
        sum(part_supply.ps_supplycost * part_supply.ps_availqty)
            / nullif(sum(part_supply.ps_availqty), 0) as unit_cost,
        parts.p_retailprice - unit_cost as unit_margin,
        unit_margin * total_available_quantity as total_margin
    from parts
    inner join part_supply
        on parts.p_partkey = part_supply.ps_partkey
    group by
        1, 2, 3, 4, 5, 6

),

ranked as (

    select
        *,
        row_number() over (
            order by total_margin desc, unit_margin desc, part_key
        ) as margin_rank
    from part_margins

)

select *
from ranked
where margin_rank <= 50
order by margin_rank
