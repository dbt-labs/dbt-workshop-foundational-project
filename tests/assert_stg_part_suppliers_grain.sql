-- A part-supplier pair must appear once to avoid multiplying inventory or cost.
select
    part_key,
    supplier_key,
    count(*) as row_count
from {{ ref('stg_part_suppliers') }}
group by part_key, supplier_key
having count(*) > 1
