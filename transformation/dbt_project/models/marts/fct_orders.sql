{{
  config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={
      "field": "order_purchase_timestamp",
      "data_type": "timestamp",
      "granularity": "month"
    },
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

with enriched_orders as (
    select * from {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
    -- Only process new orders that arrived since the last run
    where order_purchase_timestamp >= (
        select max(order_purchase_timestamp) from {{ this }}
    )
    {% endif %}
)

select
    order_id,
    {{ mask_pii('customer_id') }} as customer_id,
    {{ mask_pii('customer_unique_id') }} as customer_unique_id,
    customer_city,
    customer_state,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    total_payment_sequences,
    total_payment_value,
    max_payment_installments,
    avg_review_score,
    total_reviews,
    delivery_time_days,
    delta_delivery_vs_estimated_days
from enriched_orders