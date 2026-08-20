{{ config(materialized='view') }}

select
    count(*) as order_count

from {{ source('ecommerce_gold', 'fct_orders') }}
