-- Each part-supplier pair must identify exactly one offering.
-- Investigate source duplicates or staging grain changes if this fails.
select
    part_key,
    supplier_key,
    count(*) as row_count
from {{ ref('stg_part_suppliers') }}
group by part_key, supplier_key
having count(*) > 1
