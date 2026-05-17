with source as (
    select * from {{ source('olist_silver', 'seller') }}
)

select
    {{ col('seller_id', 'string') }} as seller_id,
    {{ col('seller_zip_code_prefix', 'string') }} as seller_zip_code_prefix,
    {{ col('seller_city', 'string') }} as seller_city,
    {{ col('seller_state', 'string') }} as seller_state,
    
    -- Metadata columns
    {{ col('event_id', 'string') }} as event_id,
    {{ col('ingested_at', 'timestamp') }} as ingested_at,
    {{ col('schema_version', 'string') }} as schema_version,
    {{ col('_silver_loaded_at', 'timestamp') }} as silver_loaded_at
from source
