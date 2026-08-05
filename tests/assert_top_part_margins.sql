select *
from {{ ref('top_part_margins') }}
where total_margin_rank > 50
   or unit_margin != unit_retail_price - weighted_average_unit_cost
   or total_margin != unit_margin * total_available_quantity
