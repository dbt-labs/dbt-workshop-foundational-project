-- Each catalog part can have multiple suppliers, but each pair must be unique.
select
    part_key,
    supplier_key,
    count(*) as row_count
from {{ ref('stg_part_suppliers') }}
group by part_key, supplier_key
having count(*) > 1
