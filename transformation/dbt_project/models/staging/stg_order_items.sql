with source as (
    select * from {{ source('olist_silver', 'order_item') }}
)

select
    {{ col('order_id', 'string') }} as order_id,
    {{ col('order_item_id', 'integer') }} as order_item_sequence,
    {{ col('product_id', 'string') }} as product_id,
    {{ col('seller_id', 'string') }} as seller_id,
    {{ col('shipping_limit_date', 'string') }} as shipping_limit_date,
    {{ col('price', 'numeric') }} as price,
    {{ col('freight_value', 'numeric') }} as freight_value,
    
    -- Metadata columns
    {{ col('event_id', 'string') }} as event_id,
    {{ col('ingested_at', 'timestamp') }} as ingested_at,
    {{ col('schema_version', 'string') }} as schema_version,
    {{ col('_silver_loaded_at', 'timestamp') }} as silver_loaded_at
from source
