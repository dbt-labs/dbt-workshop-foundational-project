{% test unique_line_item_grain(model, columns) %}

with validation as (

    select
        {% for column in columns %}
        {{ column }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        count(*) as record_count
    from {{ model }}
    group by
        {% for column in columns %}
        {{ column }}{% if not loop.last %}, {% endif %}
        {% endfor %}

),

validation_errors as (

    select *
    from validation
    where record_count > 1

)

select *
from validation_errors

{% endtest %}
