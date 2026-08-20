{{ config(materialized='table') }}

with orders as (

    select *
    from {{ source('ecommerce_gold', 'fct_orders') }}

),

dates as (

    select *
    from {{ source('ecommerce_gold', 'dim_date') }}

),

customers as (

    select *
    from {{ source('ecommerce_gold', 'dim_customers') }}

)

select
    concat(
        d.year_month,
        '||',
        coalesce(
            c.customer_state,
            'UNKNOWN'
        )
    ) as delivery_quality_key,

    d.year_month,

    coalesce(
        c.customer_state,
        'UNKNOWN'
    ) as customer_state,

    count(*) as total_orders,

    sum(
        case
            when o.order_status = 'delivered'
            then 1
            else 0
        end
    ) as delivered_orders,

    sum(
        case
            when o.is_delivered_late = true
            then 1
            else 0
        end
    ) as late_orders,

    sum(
        case
            when o.order_status = 'delivered'
             and o.is_delivered_late = false
            then 1
            else 0
        end
    ) as on_time_orders,

    round(
        100.0
        * sum(
            case
                when o.is_delivered_late = true
                then 1
                else 0
            end
        )
        / nullif(
            sum(
                case
                    when o.order_status = 'delivered'
                    then 1
                    else 0
                end
            ),
            0
        ),
        2
    ) as late_delivery_rate_pct,

    round(
        avg(
            case
                when o.order_status = 'delivered'
                then o.delivery_days
            end
        ),
        2
    ) as average_delivery_days,

    round(
        avg(o.average_review_score),
        2
    ) as average_review_score,

    round(
        sum(o.payment_total),
        2
    ) as total_payment_value

from orders as o

inner join dates as d
    on o.order_date_key = d.date_key

inner join customers as c
    on o.customer_key = c.customer_key

group by
    d.year_month,
    coalesce(
        c.customer_state,
        'UNKNOWN'
    )