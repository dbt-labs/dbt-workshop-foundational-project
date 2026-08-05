with part_supplier_margins as (

    select
        part.p_partkey as part_key,
        part.p_name as part_name,
        partsupp.ps_suppkey as supplier_key,
        part.p_retailprice - partsupp.ps_supplycost as unit_margin,
        (part.p_retailprice - partsupp.ps_supplycost) * partsupp.ps_availqty as total_margin
    from {{ ref('stg_tpch__part') }} as part
    inner join {{ ref('stg_tpch__partsupp') }} as partsupp
        on part.p_partkey = partsupp.ps_partkey

),

product_margins as (

    select
        part_key,
        part_name,
        count(*) as supplier_count,
        max(unit_margin) as best_unit_margin,
        sum(total_margin) as total_margin
    from part_supplier_margins
    group by 1, 2

),

ranked_products as (

    select
        row_number() over (
            order by total_margin desc, part_key
        ) as total_margin_rank,
        part_key,
        part_name,
        supplier_count,
        best_unit_margin,
        total_margin
    from product_margins

)

select *
from ranked_products
where total_margin_rank <= 50
order by total_margin_rank
