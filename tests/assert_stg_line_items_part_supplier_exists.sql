-- Both keys must match the same part-supplier row. Independent foreign-key
-- checks would miss invalid combinations. Nulls are covered by not_null tests.
select
    line_items.order_key,
    line_items.line_number,
    line_items.part_key,
    line_items.supplier_key
from {{ ref('stg_line_items') }} as line_items
left join {{ ref('stg_part_suppliers') }} as part_suppliers
    on line_items.part_key = part_suppliers.part_key
    and line_items.supplier_key = part_suppliers.supplier_key
where line_items.part_key is not null
    and line_items.supplier_key is not null
    and part_suppliers.part_key is null
