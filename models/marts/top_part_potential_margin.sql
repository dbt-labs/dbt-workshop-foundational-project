with parts as (

    select * from {{ ref('stg_tpch__part') }}

),

part_suppliers as (

    select * from {{ ref('stg_tpch__partsupp') }}

),

product_margins as (

    select
        parts.p_partkey as part_key,
        parts.p_name as part_name,
        parts.p_mfgr as manufacturer,
        parts.p_brand as brand,
        parts.p_type as part_type,
        parts.p_retailprice as retail_price,
        sum(part_suppliers.ps_availqty) as total_available_quantity,
        avg(parts.p_retailprice - part_suppliers.ps_supplycost) as average_unit_margin,
        sum(
            (parts.p_retailprice - part_suppliers.ps_supplycost)
            * part_suppliers.ps_availqty
        ) as potential_total_margin
    from parts
    inner join part_suppliers
        on parts.p_partkey = part_suppliers.ps_partkey
    group by
        parts.p_partkey,
        parts.p_name,
        parts.p_mfgr,
        parts.p_brand,
        parts.p_type,
        parts.p_retailprice

),

ranked_products as (

    select
        *,
        row_number() over (
            order by potential_total_margin desc, part_key
        ) as potential_margin_rank
    from product_margins

)

select *
from ranked_products
where potential_margin_rank <= 50
order by potential_total_margin desc, part_key
