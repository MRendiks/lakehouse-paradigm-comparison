with order_items as (
    select * from {{ ref('fct_order_items') }}
),
products as (
    select * from {{ ref('dim_products') }}
)

select
    p.product_category_name,
    count(distinct oi.order_id) as total_orders,
    count(oi.order_item_id) as total_items_sold,
    sum(oi.price) as total_revenue,
    avg(oi.price) as average_item_price
from products p
left join order_items oi on p.product_id = oi.product_id
where p.product_category_name is not null
group by 1