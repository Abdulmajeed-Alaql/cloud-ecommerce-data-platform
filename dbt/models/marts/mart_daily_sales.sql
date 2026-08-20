{{ config(materialized='table') }}

with orders as (

    select *
    from {{ source('ecommerce_gold', 'fct_orders') }}

),

dates as (

    select *
    from {{ source('ecommerce_gold', 'dim_date') }}

)

select
    d.date_key,
    d.full_date,
    d.year_number,
    d.quarter_number,
    d.month_number,
    d.month_name,
    d.year_month,

    count(*) as total_orders,

    sum(
        case
            when o.order_status = 'delivered'
            then 1
            else 0
        end
    ) as delivered_orders,

    sum(o.item_count) as total_items,

    round(
        sum(o.item_subtotal),
        2
    ) as item_subtotal,

    round(
        sum(o.freight_total),
        2
    ) as freight_total,

    round(
        sum(o.order_total),
        2
    ) as order_total,

    round(
        sum(o.payment_total),
        2
    ) as payment_total,

    round(
        avg(o.payment_total),
        2
    ) as average_order_value,

    sum(
        case
            when o.is_delivered_late = true
            then 1
            else 0
        end
    ) as late_orders,

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
        avg(o.average_review_score),
        2
    ) as average_review_score

from orders as o

inner join dates as d
    on o.order_date_key = d.date_key

group by
    d.date_key,
    d.full_date,
    d.year_number,
    d.quarter_number,
    d.month_number,
    d.month_name,
    d.year_month
