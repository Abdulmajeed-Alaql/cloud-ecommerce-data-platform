{{ config(materialized='table') }}

with orders as (

    select *
    from {{ source('ecommerce_gold', 'fct_orders') }}

),

customers as (

    select *
    from {{ source('ecommerce_gold', 'dim_customers') }}

)

select
    c.customer_unique_id,

    max_by(
        c.customer_city,
        o.order_purchase_timestamp
    ) as latest_customer_city,

    max_by(
        c.customer_state,
        o.order_purchase_timestamp
    ) as latest_customer_state,

    count(
        distinct o.order_id
    ) as total_orders,

    sum(
        o.item_count
    ) as total_items,

    round(
        sum(o.order_total),
        2
    ) as lifetime_order_value,

    round(
        sum(o.payment_total),
        2
    ) as lifetime_payment_value,

    round(
        avg(o.payment_total),
        2
    ) as average_order_value,

    min(
        o.order_purchase_timestamp
    ) as first_order_at,

    max(
        o.order_purchase_timestamp
    ) as latest_order_at,

    sum(
        case
            when o.is_delivered_late = true
            then 1
            else 0
        end
    ) as late_orders,

    round(
        avg(o.average_review_score),
        2
    ) as average_review_score

from orders as o

inner join customers as c
    on o.customer_key = c.customer_key

group by
    c.customer_unique_id