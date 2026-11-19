-- Each part can have multiple suppliers, but each part-supplier pair must be
-- unique. Inspect the source pair if this test fails.
select
    part_key,
    supplier_key,
    count(*) as row_count
from {{ ref('stg_part_suppliers') }}
group by part_key, supplier_key
having count(*) > 1
