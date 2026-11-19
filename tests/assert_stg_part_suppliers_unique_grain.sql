-- A supplier can supply many parts, and a part can have many suppliers.
select
    part_key,
    supplier_key,
    count(*) as record_count
from {{ ref('stg_part_suppliers') }}
group by part_key, supplier_key
having count(*) > 1
