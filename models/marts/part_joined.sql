{{
      config(
        materialized='table',
    )
}}

WITH joined AS (
    SELECT *
    FROM {{ ref('stg_tpch__part') }}
        LEFT JOIN {{ ref('stg_tpch__partsupp') }} ON p_partkey = ps_partkey
), calculate AS (
    SELECT p_retailprice - ps_supplycost AS margin, ps_availqty, p_name
    FROM joined

), total_margin AS (
    SELECT margin*ps_availqty AS total_margin, p_name
    FROM calculate
)
, rank AS (
    SELECT p_name, total_margin
    FROM total_margin
    ORDER BY total_margin DESC
    LIMIT 50
)

SELECT *
FROM rank