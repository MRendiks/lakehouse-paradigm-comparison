with source as (
    select * from {{ source('olist_silver', 'review') }}
)

select
    {{ col('review_id', 'string') }} as review_id,
    {{ col('order_id', 'string') }} as order_id,
    {{ col('review_score', 'integer') }} as review_score,
    {{ col('review_comment_title', 'string') }} as review_comment_title,
    {{ col('review_comment_message', 'string') }} as review_comment_message,
    {{ col('review_creation_date', 'timestamp') }} as review_creation_date,
    {{ col('review_answer_timestamp', 'timestamp') }} as review_answer_timestamp,
    
    -- Metadata columns
    {{ col('event_id', 'string') }} as event_id,
    {{ col('ingested_at', 'timestamp') }} as ingested_at,
    {{ col('schema_version', 'string') }} as schema_version,
    {{ col('_silver_loaded_at', 'timestamp') }} as silver_loaded_at
from source
