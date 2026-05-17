with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

payments as (
    -- Aggregate payments per order
    select
        order_id,
        count(distinct payment_sequential) as total_payment_sequences,
        sum(payment_value) as total_payment_value,
        max(payment_installments) as max_payment_installments
    from {{ ref('stg_payments') }}
    group by 1
),

reviews as (
    -- Aggregate reviews per order
    select
        order_id,
        avg(review_score) as avg_review_score,
        count(review_id) as total_reviews
    from {{ ref('stg_reviews') }}
    group by 1
)

select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    
    -- Payment Metrics
    coalesce(p.total_payment_sequences, 0) as total_payment_sequences,
    coalesce(p.total_payment_value, 0) as total_payment_value,
    coalesce(p.max_payment_installments, 0) as max_payment_installments,
    
    -- Review Metrics
    coalesce(r.avg_review_score, 0) as avg_review_score,
    coalesce(r.total_reviews, 0) as total_reviews,
    
    -- Date differences for performance benchmarking
    {{ dbt.datediff("o.order_purchase_timestamp", "o.order_delivered_customer_date", "day") }} as delivery_time_days,
    {{ dbt.datediff("o.order_estimated_delivery_date", "o.order_delivered_customer_date", "day") }} as delta_delivery_vs_estimated_days
from orders o
left join customers c on o.customer_id = c.customer_id
left join payments p on o.order_id = p.order_id
left join reviews r on o.order_id = r.order_id
