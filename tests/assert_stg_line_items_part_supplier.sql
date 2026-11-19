-- Validate the pair together so a valid part and valid supplier cannot mask an invalid pairing.
select
    line_items.order_key,
    line_items.line_number,
    line_items.part_key,
    line_items.supplier_key
from {{ ref('stg_line_items') }} as line_items
where line_items.part_key is null
    or line_items.supplier_key is null
    or not exists (
        select 1
        from {{ ref('stg_part_suppliers') }} as part_suppliers
        where part_suppliers.part_key = line_items.part_key
            and part_suppliers.supplier_key = line_items.supplier_key
    )
