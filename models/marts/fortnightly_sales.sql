-- Fortnights are fixed 14-day buckets counted from an anchor Monday.
-- There is no date_trunc('fortnight'), so we count whole 14-day steps
-- from the anchor. Override with: --vars '{fortnight_anchor_date: "YYYY-MM-DD"}'
{% set anchor_date = "cast('" ~ var('fortnight_anchor_date', '1991-12-30') ~ "' as date)" %}

with orders as (

    select *
    from {{ ref('fct_orders') }}

),

fortnights as (

    select
        *,
        {{ dbt.dateadd(
            'day',
            'floor(' ~ dbt.datediff(anchor_date, 'order_date', 'day') ~ ' / 14) * 14',
            anchor_date
        ) }} as fortnight_start_date
    from orders

),

final as (

    select
        cast(fortnight_start_date as date) as fortnight_start_date,
        cast({{ dbt.dateadd('day', 13, 'fortnight_start_date') }} as date) as fortnight_end_date,
        count(*) as order_count,
        count(distinct customer_key) as customer_count,
        sum(gross_item_sales_amount) as gross_sales_amount,
        sum(item_discount_amount) as discount_amount,
        sum(item_tax_amount) as tax_amount,
        sum(net_item_sales_amount) as net_sales_amount
    from fortnights
    group by 1, 2

)

select *
from final
order by fortnight_start_date
