with 

part_supp as(
    SELECT * from {{source('tpch','partsupp')}}
), 

part_ as(
    SELECT * from {{source('tpch','part')}}
),

combine as (
    select * from part_supp join part_
)

select * from combine
