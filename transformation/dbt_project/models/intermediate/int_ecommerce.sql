{{ config(materialized='ephemeral') }}

with stg_ecommerce as (
    select * from {{ ref('stg_ecommerce') }}
)

select
    product_id,
    sum(event_amount) as total_amount,
    count(event_id) as event_count
from stg_ecommerce
group by product_id
