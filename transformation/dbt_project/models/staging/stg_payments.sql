with source as (
    select * from {{ source('olist_silver', 'payment') }}
)

select
    {{ col('order_id', 'string') }} as order_id,
    {{ col('payment_sequential', 'integer') }} as payment_sequential,
    {{ col('payment_type', 'string') }} as payment_type,
    {{ col('payment_installments', 'integer') }} as payment_installments,
    {{ col('payment_value', 'numeric') }} as payment_value,
    
    -- Metadata columns
    {{ col('event_id', 'string') }} as event_id,
    {{ col('ingested_at', 'timestamp') }} as ingested_at,
    {{ col('schema_version', 'string') }} as schema_version,
    {{ col('_silver_loaded_at', 'timestamp') }} as silver_loaded_at
from source
