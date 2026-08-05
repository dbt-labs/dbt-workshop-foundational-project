select
    part_key,
    margin_rank
from {{ ref('top_50_parts_by_margin') }}
where margin_rank not between 1 and 50
