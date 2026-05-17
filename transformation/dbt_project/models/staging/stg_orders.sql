with source as (
    select * from {{ source('olist_silver', 'ORDER') }}
)

select
    {{ col('order_id', 'string') }} as order_id,
    {{ col('customer_id', 'string') }} as customer_id,
    {{ col('order_status', 'string') }} as order_status,
    {{ col('order_purchase_timestamp', 'timestamp') }} as order_purchase_timestamp,
    {{ col('order_approved_at', 'timestamp') }} as order_approved_at,
    {{ col('order_delivered_carrier_date', 'timestamp') }} as order_delivered_carrier_date,
    {{ col('order_delivered_customer_date', 'timestamp') }} as order_delivered_customer_date,
    {{ col('order_estimated_delivery_date', 'timestamp') }} as order_estimated_delivery_date,
    
    -- Metadata columns
    {{ col('event_id', 'string') }} as event_id,
    {{ col('ingested_at', 'timestamp') }} as ingested_at,
    {{ col('schema_version', 'string') }} as schema_version,
    {{ col('_silver_loaded_at', 'timestamp') }} as silver_loaded_at
from source
