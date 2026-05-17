with source as (
    select * from {{ source('olist_silver', 'geolocation') }}
)

select
    {{ col('geolocation_zip_code_prefix', 'string') }} as geolocation_zip_code_prefix,
    {{ col('geolocation_lat', 'numeric') }} as geolocation_lat,
    {{ col('geolocation_lng', 'numeric') }} as geolocation_lng,
    {{ col('geolocation_city', 'string') }} as geolocation_city,
    {{ col('geolocation_state', 'string') }} as geolocation_state,
    
    -- Metadata columns
    {{ col('event_id', 'string') }} as event_id,
    {{ col('ingested_at', 'timestamp') }} as ingested_at,
    {{ col('schema_version', 'string') }} as schema_version,
    {{ col('_silver_loaded_at', 'timestamp') }} as silver_loaded_at
from source
