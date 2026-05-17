with source as (
    select * from {{ source('olist_silver', 'product_category') }}
)

select
    {{ col('product_category_name', 'string') }} as product_category_name,
    {{ col('product_category_name_english', 'string') }} as product_category_name_english,
    
    -- Metadata columns
    {{ col('event_id', 'string') }} as event_id,
    {{ col('ingested_at', 'timestamp') }} as ingested_at,
    {{ col('schema_version', 'string') }} as schema_version,
    {{ col('_silver_loaded_at', 'timestamp') }} as silver_loaded_at
from source
