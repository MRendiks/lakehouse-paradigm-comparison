with order_items as (
    select * from {{ ref('fct_order_items') }}
),
sellers as (
    select * from {{ ref('dim_sellers') }}
)

select
    s.seller_id,
    s.seller_city,
    s.seller_state,
    count(distinct oi.order_id) as total_orders_fulfilled,
    count(oi.order_item_id) as total_items_sold,
    sum(oi.price) as total_revenue,
    sum(oi.freight_value) as total_freight_value,
    avg(oi.price) as average_item_price
from sellers s
left join order_items oi on s.seller_id = oi.seller_id
group by 1, 2, 3