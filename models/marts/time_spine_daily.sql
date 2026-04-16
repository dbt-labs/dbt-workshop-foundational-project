{{
    config(
        materialized='table'
    )
}}

with days as (

    {{
        dbt.date_spine(
            'day',
            "to_date('2000-01-01')",
            "to_date('2030-01-01')"
        )
    }}

),

final as (

    select
        cast(date_day as date) as date_day
    from days

)

select *
from final
where date_day >= dateadd(year, -5, current_date())
  and date_day < dateadd(day, 30, current_date())
