select
    count(*) as part_count
from {{ ref('top_50_parts_by_margin') }}
having count(*) != 50
