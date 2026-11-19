-- A part can have many suppliers and a supplier can offer many parts.
-- Duplicate pairs indicate duplicate source records or an unintended grain change.
select
    part_key,
    supplier_key,
    count(*) as row_count
from {{ ref('stg_part_suppliers') }}
group by part_key, supplier_key
having count(*) > 1
