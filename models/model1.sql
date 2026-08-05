with 

source as (
    SELECT * from {{source('tpch','partsupp')}}
),

renamed as (
    SELECT
     * 
     from source
)
SELECT * from renamed