with source as (
    select * from {{ source('olist_silver', 'product') }}
)

select
    {{ col('product_id', 'string') }} as product_id,
    {{ col('product_category_name', 'string') }} as product_category_name,
    {{ col('product_name_lenght', 'integer') }} as product_name_length,
    {{ col('product_description_lenght', 'integer') }} as product_description_length,
    {{ col('product_photos_qty', 'integer') }} as product_photos_qty,
    {{ col('product_weight_g', 'numeric') }} as product_weight_g,
    {{ col('product_length_cm', 'numeric') }} as product_length_cm,
    {{ col('product_height_cm', 'numeric') }} as product_height_cm,
    {{ col('product_width_cm', 'numeric') }} as product_width_cm,
    
    -- Metadata columns
    {{ col('event_id', 'string') }} as event_id,
    {{ col('ingested_at', 'timestamp') }} as ingested_at,
    {{ col('schema_version', 'string') }} as schema_version,
    {{ col('_silver_loaded_at', 'timestamp') }} as silver_loaded_at
from source
