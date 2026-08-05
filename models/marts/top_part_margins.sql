with part_supplier as (

    select * from {{ ref('stg_tpch__partsupp') }}

),

margins as (

    select
        ps_partkey as part_key,
        ps_suppkey as supplier_key,
        p_name as product_name,
        p_mfgr as manufacturer,
        p_brand as brand,
        p_type as product_type,
        p_size as part_size,
        p_container as container,
        p_retailprice as unit_retail_price,
        ps_supplycost as unit_cost,
        p_retailprice - ps_supplycost as unit_margin,
        ps_availqty as total_quantity,
        (p_retailprice - ps_supplycost) * ps_availqty as total_margin

    from part_supplier

),

ranked as (

    select
        *,
        row_number() over (
            order by total_margin desc, part_key, supplier_key
        ) as total_margin_rank

    from margins

)

select * from ranked
where total_margin_rank <= 50
order by total_margin_rank
