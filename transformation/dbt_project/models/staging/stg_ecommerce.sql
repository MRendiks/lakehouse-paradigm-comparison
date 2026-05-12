{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_ecommerce', 'events') }}
)

select
    cast(id as string) as event_id,
    cast(product_id as string) as product_id,
    cast(amount as numeric) as event_amount,
    cast(created_at as timestamp) as event_timestamp
from source
