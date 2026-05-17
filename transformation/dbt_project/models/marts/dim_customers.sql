with customers as (
    -- Deduplicate customers to keep one row per customer_unique_id
    select
        customer_unique_id,
        customer_city,
        customer_state,
        row_number() over (partition by customer_unique_id order by silver_loaded_at desc) as rn
    from {{ ref('stg_customers') }}
),

orders as (
    select
        customer_unique_id,
        count(distinct order_id) as total_orders,
        sum(total_payment_value) as total_lifetime_value,
        avg(avg_review_score) as avg_lifetime_review_score
    from {{ ref('int_orders_enriched') }}
    group by 1
)

select
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.total_lifetime_value, 0) as total_lifetime_value,
    coalesce(o.avg_lifetime_review_score, 0) as avg_lifetime_review_score
from customers c
left join orders o on c.customer_unique_id = o.customer_unique_id
where c.rn = 1
