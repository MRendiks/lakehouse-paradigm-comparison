with source as (
    select * from {{ source('olist_silver', 'customer') }}
)

select
    {{ col('customer_id', 'string') }} as customer_id,
    {{ col('customer_unique_id', 'string') }} as customer_unique_id,
    {{ col('customer_zip_code_prefix', 'string') }} as customer_zip_code_prefix,
    {{ col('customer_city', 'string') }} as customer_city,
    {{ col('customer_state', 'string') }} as customer_state,
    
    -- Metadata columns
    {{ col('event_id', 'string') }} as event_id,
    {{ col('ingested_at', 'timestamp') }} as ingested_at,
    {{ col('schema_version', 'string') }} as schema_version,
    {{ col('_silver_loaded_at', 'timestamp') }} as silver_loaded_at
from source
