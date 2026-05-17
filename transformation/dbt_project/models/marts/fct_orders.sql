with enriched_orders as (
    select * from {{ ref('int_orders_enriched') }}
)

select
    order_id,
    customer_id,
    customer_unique_id,
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
