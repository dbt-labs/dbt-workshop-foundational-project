with part_supplier_margins as (

    select *
    from {{ ref('int_part_supplier_margins') }}

),

part_margins as (

    select
        part_key,
        part_name,
        manufacturer,
        brand,
        part_type,
        part_size,
        container,
        unit_retail_price,
        max(unit_margin) as best_unit_margin,
        sum(available_quantity) as total_available_quantity,
        sum(total_margin) as total_margin
    from part_supplier_margins
    group by 1, 2, 3, 4, 5, 6, 7, 8

),

ranked as (

    select
        *,
        row_number() over (
            order by total_margin desc, best_unit_margin desc, part_key
        ) as margin_rank
    from part_margins

)

select *
from ranked
where margin_rank <= 50
