-- Every line item must reference an existing part-supplier pair.
-- If this fails, inspect the source lineitem and partsupp ingestion for missing pairs.
select
    line_items.order_key,
    line_items.line_number,
    line_items.part_key,
    line_items.supplier_key
from {{ ref('stg_line_items') }} as line_items
where not exists (
    select 1
    from {{ ref('stg_part_suppliers') }} as part_suppliers
    where line_items.part_key = part_suppliers.part_key
      and line_items.supplier_key = part_suppliers.supplier_key
)
