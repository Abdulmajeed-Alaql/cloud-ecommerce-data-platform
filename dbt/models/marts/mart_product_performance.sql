{{ config(materialized='table') }}

with order_items as (

    select *
    from {{ source('ecommerce_gold', 'fct_order_items') }}

),

products as (

    select *
    from {{ source('ecommerce_gold', 'dim_products') }}

)

select
    p.product_key,
    p.product_id,
    p.product_category_name,

    count(
        distinct i.order_id
    ) as total_orders,

    sum(
        i.item_quantity
    ) as units_sold,

    round(
        sum(i.item_price),
        2
    ) as item_revenue,

    round(
        sum(i.freight_value),
        2
    ) as freight_total,

    round(
        sum(i.item_total_amount),
        2
    ) as gross_sales,

    round(
        avg(i.item_price),
        2
    ) as average_item_price,

    round(
        avg(i.delivery_days),
        2
    ) as average_delivery_days,

    sum(
        case
            when i.is_delivered_late = true
            then 1
            else 0
        end
    ) as late_items

from order_items as i

inner join products as p
    on i.product_key = p.product_key

group by
    p.product_key,
    p.product_id,
    p.product_category_name