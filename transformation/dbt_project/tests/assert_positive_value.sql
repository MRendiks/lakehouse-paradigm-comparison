select
    event_id,
    event_amount
from {{ ref('stg_ecommerce') }}
where event_amount < 0
