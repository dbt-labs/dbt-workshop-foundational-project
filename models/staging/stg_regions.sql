select 
 r_regionkey as region_key
,r_name as r_name
,r_comment as r_comment
from {{source("tpch","region")}}