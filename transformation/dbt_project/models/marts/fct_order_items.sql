{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

with source_items as (
    select * from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    -- Order items have no timestamp — join to orders for incremental filtering
    where order_id in (
        select order_id from {{ ref('fct_orders') }}
    )
    {% endif %}
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    price + freight_value as total_item_value
from source_items