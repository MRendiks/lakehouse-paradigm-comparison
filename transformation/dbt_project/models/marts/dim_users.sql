{{ config(materialized='table') }}

with int_ecommerce as (
    select * from {{ ref('int_ecommerce') }}
)

select
    product_id,
    total_amount,
    event_count,
    current_timestamp() as processed_at
from int_ecommerce
