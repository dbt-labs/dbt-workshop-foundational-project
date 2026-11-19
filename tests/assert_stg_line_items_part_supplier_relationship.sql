-- Validate the supplied pair, not only the individual part and supplier keys.
select
    line_items.order_key,
    line_items.line_number,
    line_items.part_key,
    line_items.supplier_key
from {{ ref('stg_line_items') }} as line_items
left join {{ ref('stg_part_suppliers') }} as part_suppliers
    on line_items.part_key = part_suppliers.part_key
    and line_items.supplier_key = part_suppliers.supplier_key
where part_suppliers.part_key is null
